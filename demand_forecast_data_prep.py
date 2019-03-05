"""
Created on 20.11.2018
@author: aszewczyk
Function that prepares data for demand_forecast_training() and demand_forecast_predict() steps
Input:
    @first_pred_day - the outliers flights will be removed from the flights up to this day-1 (ie. end of training set), this is to train model without outliers, future flights
    (ie. for  predictions) will not have outliers removed
    @jarra - string with size of the cluster on AWS ('quinto', 'tercio', 'mediana', 'mass')
    @step - 'train' or 'test', only to differentiate names of LOG_FILE, 'eval' for evaluation only (remove outliers in whole data set)
    @verbose - should print logger messages on the screen and save them to .log file?
    @checks - should calculate advanced metrics, @mlflow_params_extra?
Returns:
    @data - spark d.f. with model data prepared
    @logger - logger connection to use in next steps of train or predict
    @mlflow_params - dictionary with parameters for mlflow, if @checks=True
Example:
    data, logger, mlflow_params = demand_forecast_data_prep(first_pred_day=False,
                              jarra='quinto',
                              step='train',
                              verbose=True,
                              checks=False)
    Prepares data for train step (demand_forecast_training())
"""
import os
from datetime import datetime
import logging
from lib.calc_conversion_rates import calc_conversion_rates
from lib.clean_competition_data import clean_competition_data
from lib.clean_demand_data import clean_demand_data
from lib.clean_sales_last_days_data import clean_sales_last_days_data
from lib.clean_sales_last_year_data import clean_sales_last_year_data
from lib.create_cyclical_features import create_cyclical_features
from lib.create_ranking_variables import create_ranking_variables
from lib.create_reduced_airport_pair_variable import create_reduced_airport_pair_variable
from lib.create_spark_session import create_spark_session
from lib.define_features import define_features
from lib.define_variable_names import define_variable_names
from lib.join_demand_tables import join_demand_tables
from lib.save_input_model_data_to_local import save_input_model_data_to_local
from lib.treat_outliers import treat_outliers
from pyspark.sql.types import  IntegerType
from pyspark.sql.functions import col, sum
from ConfigParser import SafeConfigParser
#from utils import remove_file_if_exists
#from ..demand_forecast_pred import get_data
#MODEL_CONFIG_FILE=get_data("config/demand_forecast_config.conf")
local_path=os.getcwd() + '/'
MODEL_CONFIG_FILE=local_path+'data/config/demand_forecast_config.conf'
def demand_forecast_data_prep(first_pred_day=None,
                              jarra='quinto',
                              step='train',
                              verbose=True,
                              checks=False):
    try:
        if step=='train':
            LOG_FILE=local_path+'logs/demand_forecast_train.log'
        elif step=='predict':
            LOG_FILE=local_path+'logs/demand_forecast_predict.log'
        elif step=='eval':
            LOG_FILE=local_path+'logs/demand_forecast_eval.log'

        #remove_file_if_exists(LOG_FILE)
        if step == 'eval':
            model_evaluation=True
        else:
            model_evaluation=False
        # set paths
        # config .ini file has to be in the same directory as df script
        start_all = datetime.now()
        if first_pred_day is not None:
            split_value = first_pred_day
            #split_value = datetime.strptime(split_value, '%Y-%m-%d')
        else:
            split_value = datetime.today()
            first_pred_day = split_value.strftime('%Y-%m-%d')
            split_value = split_value.strftime('%Y-%m-%d')
        #set logging
        print(first_pred_day)
        if verbose:
            logging.basicConfig(level=logging.INFO)
            logger = logging.getLogger(__name__)
            formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
            hdlr = logging.FileHandler(LOG_FILE) #logging handle
            hdlr.setFormatter(formatter)
            logger.addHandler(hdlr)
            logger.info('Logging start')
            logger.info('Data Preparation for Demand Forecast start')
        else:
            logger = False
        #connect to Spark Session
        spark = create_spark_session(jarra=jarra,
                                     verbose=verbose,
                                     logger=logger)
        if verbose:
            logger.info('Load databases start')
        ###LOAD DATA from hive as spark data frames
        ds_flight_master = spark.sql("""SELECT * FROM revenue.ds_flight_master""")
        ds_flight_master = ds_flight_master.withColumn("id_season", ds_flight_master["id_season"].cast(IntegerType()))
        ds_cumulative_data = spark.sql("""SELECT * FROM revenue.ds_cumulative_data where days_in_advance = -1""")
        ds_sales_last_year = spark.sql("""SELECT * FROM revenue.ds_sales_last_year""")
        ds_sales_last_days_seasonal = spark.sql("""SELECT * FROM revenue.ds_sales_last_days_seasonal""")
        ds_sales_last_days_airport_pair = spark.sql("""SELECT * FROM revenue.ds_sales_last_days_airport_pair""")
        ds_competitor_data_2h = spark.sql("""SELECT * FROM revenue.ds_competitor_price_2h where is_vy_carrier = false""")
        ds_competitor_data_6h = spark.sql("""SELECT * FROM revenue.ds_competitor_price_6h where is_vy_carrier = false""")
        ds_competitor_data_day = spark.sql("""SELECT * FROM revenue.ds_competitor_price_day where is_vy_carrier = false""")
        ds_market_share_data = spark.sql("""SELECT * FROM revenue.ds_market_share_vy_2h""")
        ds_looktobook_cumulative_data = spark.sql("""SELECT * FROM revenue.ds_looktobook_cumulative where ndo = 0""")
        ds_holidays_data = spark.sql("""SELECT 1 as holidays""")
        if verbose:
            logger.info('Load databases end')
            logger.info('Load parameters start')
        #read parameters from config file
        parser = SafeConfigParser()
        parser.read(MODEL_CONFIG_FILE)
        remove_outliers = parser.getboolean('model_params', 'remove_outliers')
        local_save_path = parser.get('save_params', 'local_save_path')
        first_day_data = parser.get('date_values', 'first_day_data')
        if not os.path.exists(local_save_path):
            os.makedirs(local_save_path)
        if verbose:
            logger.info('Load parameters end')
        #get variable names used throughout the script
        named_variables_lists = define_variable_names(verbose=verbose,
                                                      logger=logger)
        ###DATA ENGINEERING
        #data only since beginning of 2017
        ds_flight_master = ds_flight_master.filter(col('dt_flight_date_local') >= first_day_data)
        #create new categorical variable: cd_airport_pair_reduced. Largest (by total revenue) count_threshold segments have the same value as cd_airport_pair,
        #all the smaller segments are given value: 'minor_airport_pairs'
        ds_flight_master = create_reduced_airport_pair_variable(ds_flight_master,
                                                                count_threshold=50,
                                                                verbose=verbose,
                                                                logger=logger)
        #create ranking variables for all the target features as sum of given feature over cd_airport_pair using all data
        ds_flight_master = create_ranking_variables(ds_flight_master,
                         ranking_cols=['revenue_tickets', 'pax_seat', 'ancis'],
                         ranking_by_col='cd_airport_pair',
                         ranking_time_variable=False,
                         ranking_time_amount=False,
                         verbose=True,
                         logger=logger)
        #cleans sales last year data
        ds_sales_last_year = clean_sales_last_year_data(ds_sales_last_year,
                                                        named_variables_lists,
                                                        verbose=verbose,
                                                        logger=logger)
        #cleans sales last days data
        ds_sales_last_days_seasonal, ds_sales_last_days_airport_pair = clean_sales_last_days_data(ds_sales_last_days_seasonal,
                                                                                                  ds_sales_last_days_airport_pair,
                                                                                                  named_variables_lists,
                                                                                                  verbose=verbose,
                                                                                                  logger=logger)
        #joins all the tables together (left join to the flight master table)
        demand_data = join_demand_tables(ds_flight_master,
                                         ds_cumulative_data,
                                         ds_sales_last_year,
                                         ds_sales_last_days_seasonal,
                                         ds_sales_last_days_airport_pair,
                                         ds_looktobook_cumulative_data,
                                         ds_competitor_data_2h,
                                         ds_competitor_data_6h,
                                         ds_competitor_data_day,
                                         ds_market_share_data,
                                         ds_holidays_data,
                                         verbose=verbose,
                                         logger=logger)
        #feat. engineering cyclical features, first get list with c.f. from define_features(), not depend on target var.
        features_list, features_list_basic_model, ohe_variables_in, cyclical_variables =\
        define_features(model_complex=False,
                        use_clustered_data_sets=False,
                        col_target=False,
                        verbose=True,
                        logger=logger)
        demand_data = create_cyclical_features(demand_data,
                                               cyclical_variables,
                                               drop_orig_vars=False,
                                               verbose=True,
                                               logger=logger)
        #check if number of lines of ds_flight_master and demand_data (after joining all the tables) agrees
        demand_data.cache()
        #clean data, adds new variables (like l.f. and yields), then remove unnecessary variables (ie. the ones used to calculate new variables)
        demand_data = clean_demand_data(demand_data,
                                        named_variables_lists,
                                        verbose=verbose,
                                        logger=logger)
        #clean competition price data (ie. where no flights around 2h, use around 6h price, if noe the whole day price, if not vueling price)
        demand_data = clean_competition_data(demand_data,
                                             named_variables_lists,
                                             verbose=verbose,
                                             logger=logger)
        #calculate pax conversion rates for L2B variables and afterwards pax from views variables
        #if (model_complexity == 'second') or (model_complexity == 'second_simplified') or (model_complexity == 'third') or (model_complexity == 'fourth'):
        demand_data = calc_conversion_rates(demand_data,
                                            named_variables_lists,
                                            verbose=verbose,
                                            logger=logger)
        #select only Vueling flights
        data = demand_data.filter(col('cd_carrier') == 'VY')
        #remove outliers from data
        if remove_outliers:
            if verbose:
                if model_evaluation:
                    logger.info('Outliers in train and test set will be removed')
                else:
                    logger.info('Outliers in train set will be removed')
            data = treat_outliers(data,
                                  named_variables_lists,
                                  model_evaluation=model_evaluation,
                                  split_value=split_value,
                                  min_lf=0.25,  #1st decil (10%)
                                  min_yield=10, #1st decil (10%)
                                  max_lf=1.25,
                                  max_price=600,
                                  max_yield=600,
                                  max_yield_ancis=70,
                                  verbose=verbose,
                                  logger=logger)
        print(data.count())
        #for the first model save also the model input data to local (only for pax_seat, to avoid multiple saves of the same data)
        #if save_model_data_to_local: #and col_target=='pax_seat':
        if verbose:
            logger.info('Caching data start')
        data.cache()
        if verbose:
            logger.info('Caching data end')
        #save data to local
        save_input_model_data_to_local(data,
                                       spark,
                                       first_pred_day,
                                       local_save_path,
                                       verbose,
                                       logger)
        if verbose:
            logger.info('All data saved in '+local_save_path)
        end_all = datetime.now()
        logger.info('Random Forest, date preparation, time: ' + str(end_all-start_all))
        #print metrics and create mlflow metrics
        if verbose and checks:
            param_pax = data\
                        .groupBy("dt_flight_year_month")\
                        .agg(sum('pax_seat'))
            param_revenue = data\
                            .groupBy("dt_flight_year_month")\
                            .agg(sum('revenue_tickets'))
            param_ancis = data\
                          .groupBy("dt_flight_year_month")\
                          .agg(sum('ancis'))
            logger.info('Check: target variables sum each month:')
            logger.info('-pax:')
            logger.info(param_pax.sort("dt_flight_year_month").toPandas())
            logger.info('-revenue_tickets:')
            logger.info(param_revenue.sort("dt_flight_year_month").toPandas())
            logger.info('-ancis:')
            logger.info(param_ancis.sort("dt_flight_year_month").toPandas())
            countFlightMaster = ds_flight_master.count()
            DemandDataCount = demand_data.count()
            minFlightDate = str(ds_flight_master\
                                .agg({'dt_flight_date_local': 'min'})\
                                .collect()[0][0])
            maxFlightDate = str(ds_flight_master\
                                .agg({'dt_flight_date_local': 'max'})\
                                .collect()[0][0])
            checkDemandDataDuplicates = abs(DemandDataCount - countFlightMaster)
            logger.info('Number of rows (distinct flights) in ds_flight_master: ' + str(countFlightMaster))
            logger.info('Number of rows (distinct flights) after joining all the tables: ' + str(DemandDataCount))
            logger.info('Number of duplicated rows/flights after joining all the tables: ' + str(checkDemandDataDuplicates))
            mlflow_params = {'duplicates': checkDemandDataDuplicates,
                             'num_flights': countFlightMaster,
                             'time_seconds': (end_all-start_all).total_seconds(),
                             'date_min': minFlightDate,
                             'date_max': maxFlightDate
                            }
        else:
            mlflow_params = {}
        #spark.stop()
        #if verbose:
        #    logger.info('Spark Session stopped')
    except Exception:
        logger.exception("Fatal error in demand_forecast_data_prep()")
        raise
    return(data, logger, mlflow_params)
