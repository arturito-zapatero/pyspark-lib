import os
import sys
import pandas as pd
import numpy as np
import boto3
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
from haversine import haversine
import category_encoders as ce
from sklearn.linear_model import ElasticNet
from datetime import date, timedelta
from sklearn.pipeline import Pipeline

from lib.store_pipeline_to_s3 import store_pipeline_to_s3
from lib.dpo_clusters_funcs import assign_data_to_clusters, calc_shortest_dist_between_clusters
from lib.kml_utils import extract_coords_from_kml
from lib.map_utils import get_rows_inside_polygon
from lib.dpo_train_test_split import dpo_pred_data_split
from lib.dpo_stacking_model import dpo_stacking_train_models
from lib.dpo_create_full_cluster_data import dpo_create_full_cluster_data
import lib.dpo_athena_funcs as athena
import lib.dpo_weather_funcs as weather
import lib.base_utils as lutil


def dpo_training_arrival(config_file_base, logger=None):
    try:
        logger.info('dpo_training_arrival() start')

        # Check if the model evaluation config files are already created (to stop run for the first deployment of a
        # city)
        s3_resource = boto3.resource('s3')
        dpo_bucket = s3_resource.Bucket(os.environ['s3_dpo_bucket'])
        key_eval_daily = 'results/daily_model/model_evaluation/' + config_file_base + '_daily_models.config'
        objs_eval_daily = list(dpo_bucket.objects.filter(Prefix=key_eval_daily))
        if len(objs_eval_daily) < 1:
            print("Evaluation files still not created, exits")
            sys.exit(0)

        # Load config from S3
        logger.info('load config from S3: start')
        config_file_path = os.environ['environment'] + '/' + os.environ['project'] + \
                           '/config/' + config_file_base + '.config'
        logger.info('code_bucket is: ' + str(os.environ['code_bucket']))
        logger.info('config_file_path is: ' + str(config_file_path))
        config = lutil.read_s3_config(os.environ['code_bucket'], config_file_path)
        models_config_file_path = 'results/daily_model/model_evaluation/' + \
                                  config_file_base + '_daily_models.config'
        logger.info('models_config_file_path is: ' + str(models_config_file_path))
        output_kml = config['DEFAULT']['output_kml']
        city = config['DEFAULT']['city']
        city_name = config['DEFAULT']['city_name']
        city_type = config['DEFAULT']['city_type']
        oa_layer_name = config['DEFAULT']['oa_layer_name']
        release_layer_name = config['DEFAULT']['release_layer_name']
        first_training_day = pd.Timestamp(config['DEFAULT']['first_training_day'])
        last_training_day = pd.Timestamp(date.today() - timedelta(days=1))
        train_sample_size = float(config['DEFAULT']['train_sample_size'])
        first_pred_day = config['DEFAULT']['first_pred_day']
        if first_pred_day == 'today':
            first_pred_day = pd.Timestamp(date.today())
        else:
            first_pred_day = pd.Timestamp(first_pred_day)
        pred_days = int(config['DEFAULT']['pred_days'])
        weather_db = config['DEFAULT']['weather_db']
        weather_historical_sql_file = config['DEFAULT']['weather_historical_sql_file']
        weather_forecast_sql_file = config['DEFAULT']['weather_forecast_sql_file']

        # Load variables, target and id variables from config
        col_target = config['COLUMNS_DEFINITIONS']['col_target_daily_arrival']
        col_predict = 'pred_' + col_target
        col_predict_train = 'pred_train_' + col_target
        cols_id = config['COLUMNS_DEFINITIONS']['cols_id_daily_arrival']
        cols_id = [str(i) for i in cols_id.split(',')]
        col_date = config['COLUMNS_DEFINITIONS']['col_day_daily_arrival']
        cols_numerical = config['COLUMNS_DEFINITIONS']['cols_numerical_daily_arrival']
        cols_numerical = [str(i) for i in cols_numerical.split(',')]
        cols_categorical = config['COLUMNS_DEFINITIONS']['cols_categorical_daily_arrival']
        cols_categorical = [str(i) for i in cols_categorical.split(',')]
        if cols_categorical == ['']:
            cols_categorical = []
        cols_cyclical_in = config['COLUMNS_DEFINITIONS']['cols_cyclical_in_daily_arrival']
        cols_cyclical_in = [str(i) for i in cols_cyclical_in.split(',')]
        if cols_cyclical_in == ['']:
            cols_cyclical_in = []

        # Deal with cyclical variables
        cols_variables_sin = [s + '_sin' for s in cols_cyclical_in]
        cols_variables_cos = [s + '_cos' for s in cols_cyclical_in]
        cols_cyclical_out = cols_variables_sin + cols_variables_cos
        cols_numerical = cols_numerical + cols_cyclical_out

        # get best models' parameters from config
        models_config = lutil.read_s3_config(os.environ['s3_dpo_bucket'], models_config_file_path)
        best_rf_model_params = {'max_depth': [int(models_config['arrivals_rf_best_params']['max_depth'])],
                                'max_features': [float(models_config['arrivals_rf_best_params']['max_features'])],
                                'n_estimators': [int(models_config['arrivals_rf_best_params']['n_estimators'])],
                                'min_samples_leaf': [int(models_config['arrivals_rf_best_params']['min_samples_leaf'])]
                                }

        best_xgb_model_params = {'max_depth': [int(models_config['arrivals_xgb_best_params']['max_depth'])],
                                 'learning_rate': [float(models_config['arrivals_xgb_best_params']['learning_rate'])],
                                 'n_estimators': [int(models_config['arrivals_xgb_best_params']['n_estimators'])],
                                 'min_samples_leaf': [
                                     int(models_config['arrivals_xgb_best_params']['min_samples_leaf'])],
                                 'colsample_bytree': [
                                     float(models_config['arrivals_xgb_best_params']['colsample_bytree'])],
                                 'subsample': [float(models_config['arrivals_xgb_best_params']['subsample'])]
                                 }

        best_svr_model_params = {'kernel': [models_config['arrivals_svr_best_params']['kernel']],
                                 'degree': [float(models_config['arrivals_svr_best_params']['degree'])],
                                 'C': [float(models_config['arrivals_svr_best_params']['C'])],
                                 'gamma': [float(models_config['arrivals_svr_best_params']['gamma'])]
                                 }
        best_stacking_model_params = {'l1_ratio': [float(models_config['arrivals_stacking_best_params']['l1_ratio'])],
                                      'alpha': [float(models_config['arrivals_stacking_best_params']['alpha'])]
                                      }

        # Download .kml from S3
        tmp_kml_file_path = 'tmp/clustering_map.kml'
        s3 = boto3.client('s3')

        # Create tmp directory if don't exists
        if not os.path.exists('tmp'):
            os.mkdir('tmp')
        s3.download_file(os.environ['s3_dpo_bucket'],
                         'maps/' + city_name + '/' + output_kml + '_clustering.kml',
                         tmp_kml_file_path)

        # Load data from RS using S3 query
        logger.info('load data from S3 start')
        fkey = 'data/input_data/arrivals/' + city_name + '/arrivals_data.csv'
        fkey = fkey.replace(" ", "_")
        s3.download_file(os.environ['s3_dpo_bucket'], fkey, 'tmp/arrivals_data.csv')
        arrivals_data = pd.read_csv('tmp/arrivals_data.csv')
        arrivals_data['ride_date'] = pd.to_datetime(arrivals_data['ride_date'])
        arrivals_data['ride_start_timestamp'] = pd.to_datetime(arrivals_data['ride_start_timestamp'])
        arrivals_data['ride_end_timestamp'] = pd.to_datetime(arrivals_data['ride_end_timestamp'])

        # Filter
        arrivals_data = arrivals_data.query("ride_date>=@first_training_day and ride_date<=@last_training_day") \
            .sample(frac=train_sample_size) \
            .reset_index().drop('index', axis=1)

        # Extract OA from .kml data
        arrivals_data = arrivals_data.reset_index() \
            .drop('index',
                  axis=1)
        oa_coords_dfs_list = extract_coords_from_kml(tmp_kml_file_path,
                                                     folder_name=oa_layer_name,
                                                     coords_type='polygon')

        # Create OA polygon out of list
        oa_coords_polygon_list = [Polygon(np.array(oa_coords_dfs_list[i]))
                                  for i in range(len(oa_coords_dfs_list))]

        # Remove points outside the OA
        in_oa_demand_data = get_rows_inside_polygon(arrivals_data,
                                                    'ride_end_longitude',
                                                    'ride_end_latitude',
                                                    oa_coords_polygon_list[0]
                                                    )[0]
        in_oa_demand_data = in_oa_demand_data.reset_index() \
            .drop('index',
                  axis=1)

        # extract release points/centers from .kml file
        release_coords_dfs_list = extract_coords_from_kml(tmp_kml_file_path,
                                                          folder_name=release_layer_name,
                                                          coords_type='points')
        release_coords_point_list = [Point(float(release_coords_dfs_list[i].loc[0, 'Longitude']),
                                           float(release_coords_dfs_list[i].loc[0, 'Latitude']))
                                     for i in range(len(release_coords_dfs_list))]

        # create df with DPs/centers using data from .kml
        centers = pd.DataFrame(columns=['cluster_longitude', 'cluster_latitude', 'cluster_id'])
        for i in range(len(release_coords_dfs_list)):
            release_coords_dfs_list[i].columns = ['cluster_longitude', 'cluster_latitude', 'cluster_id']
            centers = centers.append(release_coords_dfs_list[i])
        centers = centers.reset_index().drop('index', axis=1)
        centers['cluster_longitude'] = centers['cluster_longitude'].astype('float')
        centers['cluster_latitude'] = centers['cluster_latitude'].astype('float')
        centers, combs, agg_combs = calc_shortest_dist_between_clusters(centers)

        # assign data to precalculated centers
        arrivals_data_city = assign_data_to_clusters(data=in_oa_demand_data,
                                                     centers=centers,
                                                     col_cluster_centers='cluster_id',
                                                     col_cluster_data='cluster_id',
                                                     cols_coords_centers=['cluster_longitude', 'cluster_latitude'],
                                                     cols_coords_data=['ride_end_longitude', 'ride_end_latitude'])

        # merge information from centers df
        arrivals_data_city = arrivals_data_city.merge(centers, how='left', on='cluster_id')

        # calculate avg distance from scooter to cluster center by cluster
        arrivals_data_city['distance_to_center'] = arrivals_data_city.apply(lambda x:
                                                                            haversine((x['cluster_longitude'],
                                                                                       x['cluster_latitude']),
                                                                                      (x['ride_end_longitude'],
                                                                                       x['ride_end_latitude']),
                                                                                      unit='m'),
                                                                            axis=1,
                                                                            )
        arrivals_data_means = arrivals_data_city \
            .groupby(['cluster_id'])[['ride_distance_kms', 'dist_to_closest_cluster', 'distance_to_center',
                                      'ride_duration_secs', 'ride_end_battery_load', 'ride_start_battery_load']] \
            .mean() \
            .reset_index()
        arrivals_data_means.rename(columns={'distance_to_center': 'avg_dist_to_center'}, inplace=True)

        # group data by clusters
        clustered_arrival_data = arrivals_data_city \
            .groupby(['ride_date', 'ride_iso_day_of_week', 'ride_iso_calendar_week', 'ride_month',
                      'cluster_id', 'cluster_longitude', 'cluster_latitude'])['event_count'] \
            .sum() \
            .reset_index()

        # create all possible combination of dates and clusters, assign 0s to cluster-days w/o events
        clustered_arrival_data = dpo_create_full_cluster_data(clustered_arrival_data,
                                                              first_training_day,
                                                              last_training_day,
                                                              col_cluster_id='cluster_id',
                                                              col_event_date='ride_date')

        clustered_arrival_data = clustered_arrival_data.merge(arrivals_data_means, how='left', on='cluster_id')

        # add mean count per cluster variables and merge
        clusters_mean_counts = clustered_arrival_data \
            .groupby('cluster_id')['event_count'] \
            .mean() \
            .reset_index() \
            .rename(columns={'event_count': 'mean_event_count'})
        clustered_arrival_data = clustered_arrival_data \
            .merge(clusters_mean_counts,
                   on=['cluster_id'],
                   how='left')

        clustered_arrival_data['ride_city'] = city_name

        # Add variable scoots deployed per city-day
        n_deployed_per_city_day = arrivals_data_city.groupby(['ride_city', 'ride_date'])['n_deployed'] \
            .mean() \
            .reset_index()
        clustered_arrival_data = clustered_arrival_data \
            .merge(n_deployed_per_city_day, on=['ride_city', 'ride_date'], how='left')

        clustered_arrival_data_save = clustered_arrival_data.copy()

        # Add lag variables
        clustered_arrival_data['ride_date_minus_one_day'] = clustered_arrival_data[col_date] + \
                                                            pd.to_timedelta(1, unit='d')
        clustered_arrival_data['ride_date_minus_one_week'] = clustered_arrival_data[col_date] + \
                                                             pd.to_timedelta(7, unit='d')
        clustered_arrival_data['ride_date_minus_two_weeks'] = clustered_arrival_data[col_date] + \
                                                              pd.to_timedelta(14, unit='d')
        clustered_arrival_data['ride_date_minus_pred_days'] = clustered_arrival_data[col_date] - \
                                                              pd.to_timedelta(pred_days, unit='d')

        clustered_arrival_data = clustered_arrival_data \
            .merge(clustered_arrival_data. \
                   loc[:, ['ride_date_minus_one_week', 'cluster_id', 'event_count']] \
                   .rename(columns={'event_count': 'event_count_week_ago'}, inplace=False),
                   left_on=['ride_date', 'cluster_id'],
                   right_on=['ride_date_minus_one_week', 'cluster_id'],
                   how='left') \
            .drop(['ride_date_minus_one_week_x', 'ride_date_minus_one_week_y'], axis=1)
        clustered_arrival_data = clustered_arrival_data \
            .merge(clustered_arrival_data. \
                   loc[:, ['ride_date_minus_two_weeks', 'cluster_id', 'event_count']] \
                   .rename(columns={'event_count': 'event_count_two_weeks_ago'}, inplace=False),
                   left_on=['ride_date', 'cluster_id'],
                   right_on=['ride_date_minus_two_weeks', 'cluster_id'],
                   how='left') \
            .drop(['ride_date_minus_two_weeks_x', 'ride_date_minus_two_weeks_y'], axis=1)

        # Add lag rolling variables
        weekly_clustered_running_median = clustered_arrival_data \
            .sort_values(col_date) \
            .groupby(['cluster_id'])['event_count'] \
            .rolling(7, center=False) \
            .mean() \
            .reset_index() \
            .rename(columns={'level_1': 'index', 'event_count': 'weekly_clustered_running_median_tmp'}) \
            .set_index('index') \
            .merge(clustered_arrival_data[col_date], left_index=True, right_index=True)
        monthly_clustered_running_median = clustered_arrival_data \
            .sort_values(col_date) \
            .groupby(['cluster_id'])['event_count'] \
            .rolling(30, center=False) \
            .mean() \
            .reset_index() \
            .rename(columns={'level_1': 'index', 'event_count': 'monthly_clustered_running_median_tmp'}) \
            .set_index('index') \
            .merge(clustered_arrival_data[col_date], left_index=True, right_index=True)

        # Merge rolling variables to data df
        clustered_arrival_data = clustered_arrival_data \
            .merge(weekly_clustered_running_median,
                   left_on=['ride_date_minus_pred_days', 'cluster_id'],
                   right_on=[col_date, 'cluster_id']) \
            .rename(columns={col_date + '_x': col_date}) \
            .merge(monthly_clustered_running_median,
                   left_on=['ride_date_minus_pred_days', 'cluster_id'],
                   right_on=[col_date, 'cluster_id']) \
            .rename(columns={col_date + '_x': col_date,
                             'weekly_clustered_running_median_tmp': 'weekly_clustered_running_median',
                             'monthly_clustered_running_median_tmp': 'monthly_clustered_running_median'}) \
            .drop([col_date + '_y', 'ride_date_minus_pred_days'], axis=1)

        # Add trend variable
        date_range = pd.date_range(clustered_arrival_data.ride_date.min(),
                                   clustered_arrival_data.ride_date.max())
        day_trend_variable_tmp = []
        for i in range(len(date_range)):
            day_trend_variable_tmp.append([date_range[i].date(), i])
        day_trend_variable = pd.DataFrame(day_trend_variable_tmp, columns=['ride_date', 'day_trend'])
        day_trend_variable.ride_date = pd.to_datetime(day_trend_variable.ride_date)
        clustered_arrival_data = clustered_arrival_data \
            .merge(day_trend_variable, on='ride_date', how='left')

        # Add weather variables
        weather_historical = athena.run_athena_query_from_file(weather_historical_sql_file,
                                                               weather_db, city=city_name,
                                                               min_date=first_training_day.strftime('%Y-%m-%d'))

        weather_historical['weather_timestamp'] = pd.to_datetime(weather_historical['weather_timestamp'])
        weather_historical['weather_date'] = pd.to_datetime(weather_historical['weather_date'])
        weather_historical = weather.dpo_weather_unify_types(weather_historical)

        weather_historical_daily, weather_forecast_daily = weather.dpo_weather_create_daily_data(weather_historical,
                                                                                                 None,
                                                                                                 data_type='historical')

        weather_historical_daily = weather_historical_daily.loc[
                                   weather_historical_daily['weather_date'] < pd.Timestamp(date.today()), :]

        clustered_arrival_data = clustered_arrival_data.merge(weather_historical_daily,
                                                              left_on=[col_date],
                                                              right_on=['weather_date'],
                                                              how='left')

        # OHE + create pipelines
        pipeline_ohe = Pipeline([('ohe_encoder',
                                  ce.OneHotEncoder(handle_unknown='ignore',
                                                   use_cat_names=True,
                                                   cols=cols_categorical))])
        pipeline_ohe = pipeline_ohe.fit(clustered_arrival_data)
        clustered_arrival_data_ohe = pipeline_ohe.transform(clustered_arrival_data)
        cols_ohe_out = list(set(list(clustered_arrival_data_ohe.columns)) - set(list(clustered_arrival_data.columns)))
        cols_ohe_out.sort()

        # To avoid multicolinearity, should be done autom.
        cols_ohe_out.remove('event_iso_day_of_week_1.0')

        cols_features_full = cols_numerical + cols_ohe_out
        data = clustered_arrival_data_ohe.loc[:, cols_features_full + cols_id + [col_target]]

        # data split for train and prediction, we need full sample size for pred
        X_train, y_train, X_pred = dpo_pred_data_split(data,
                                                       cols_features_full,
                                                       col_target,
                                                       first_pred_day,
                                                       pred_days,
                                                       train_sample_size=1.0,
                                                       col_date='ride_date',
                                                       fillna=True,
                                                       cols_to_fillna=['n_deployed'])

        # run RF, XGB and SVR models and combine models' results for stacking model
        X_train_stacking, pipeline_rf, pipeline_xgb, pipeline_svr = \
            dpo_stacking_train_models(X_train,
                                      y_train,
                                      data,
                                      col_target,
                                      col_predict,
                                      col_predict_train,
                                      cols_id,
                                      best_rf_model_params,
                                      best_xgb_model_params,
                                      best_svr_model_params,
                                      n_jobs=5,
                                      svr_train_sample_size=0.01
                                      )

        # obtain final model using the stacking
        stacking_columns_train = ['rf_' + col_predict_train, 'xgb_' + col_predict_train, 'svr_' + col_predict_train]
        stacking_columns_test = ['rf_' + col_predict, 'xgb_' + col_predict, 'svr_' + col_predict]
        elastic = ElasticNet(l1_ratio=best_stacking_model_params['l1_ratio'][0],
                             alpha=best_stacking_model_params['alpha'][0],
                             max_iter=10000)
        pipeline_elastic = Pipeline([('regression', elastic)])
        pipeline_elastic.fit(X_train_stacking[stacking_columns_train], y_train)

        # save the model pred results and pipelines into S3
        tmp_arrival_model_file_base_name = date.today().strftime(
            "%Y-%m-%d") + '_' + config_file_base + '_arrivals_model_'
        lutil.store_df_s3(clustered_arrival_data_save,
                          os.environ['s3_dpo_bucket'],
                          'data/daily_model_data/arrival/' +
                          city_name,
                          tmp_arrival_model_file_base_name + 'data',
                          format='csv',
                          add_timestamp=False,
                          delete_files=True)
        store_pipeline_to_s3(pipeline_ohe,
                             os.environ['s3_dpo_bucket'],
                             file_path='models/daily_model/arrival/' + city_name + '/ohe/',
                             file_base_name=tmp_arrival_model_file_base_name + 'ohe',
                             file_format='.sav',
                             delete_files=False)
        store_pipeline_to_s3(pipeline_rf,
                             os.environ['s3_dpo_bucket'],
                             file_path='models/daily_model/arrival/' + city_name + '/rf/',
                             file_base_name=tmp_arrival_model_file_base_name + 'rf',
                             file_format='.sav',
                             delete_files=False)
        store_pipeline_to_s3(pipeline_xgb,
                             os.environ['s3_dpo_bucket'],
                             file_path='models/daily_model/arrival/' + city_name + '/xgb/',
                             file_base_name=tmp_arrival_model_file_base_name + 'xgb',
                             file_format='.sav',
                             delete_files=False)
        store_pipeline_to_s3(pipeline_svr,
                             os.environ['s3_dpo_bucket'],
                             file_path='models/daily_model/arrival/' + city_name + '/svr/',
                             file_base_name=tmp_arrival_model_file_base_name + 'svr',
                             file_format='.sav',
                             delete_files=False)
        store_pipeline_to_s3(pipeline_elastic,
                             os.environ['s3_dpo_bucket'],
                             file_path='models/daily_model/arrival/' + city_name + '/stacking/',
                             file_base_name=tmp_arrival_model_file_base_name + 'stack',
                             file_format='.sav',
                             delete_files=False)

        mlflow_params_dict = {
            'City Name': [city_name],
            'City ID': [city],
            'City Type': [city_type]
        }

        logger.info("dpo_training_arrival() end")
    except Exception:
        logger.info("Fatal error in dpo_training_arrival()")
        raise
    return mlflow_params_dict
