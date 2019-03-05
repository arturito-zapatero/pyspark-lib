"""
Created on 20.11.2018
@author: aszewczyk
Function that prepares (e.g. OHE) data already partially processed in demand_forecast_data_prep() step, trains model using data until @first_pred_day-1 for @col_target,
returns data prep and model pipelines, as well as saves them in S3
Input:
    @data - spark d.f. with model data prepared in demand_forecast_data_prep() step (we apply here date filters to get dates we need depending on first_pred_day)
    @col_target - string with target variable (pax_seat, revenue_tickets or ancis)
    @first_pred_day - last day-1 of data used for training, if not given today is set
    @jarra - string with size of the cluster on AWS ('quinto', 'tercio', 'mediana', 'mass')
    @logger - logger connection opened in demand_forecast_data_prep() step
    @verbose - should print logger messages on the screen and save them to .log file?
    @checks - should calculate advanced metrics, @mlflow_params_extra?
Returns:
    @pipelinePrepList - list with data preparation pipelines (StringIndexer, OHE etc.) for each of the models with full features list
    @pipelinePrepBasicList - list with data preparation pipelines (StringIndexer, OHE etc.) for each of the models with basic features list
    @fitList - list with RF models trained pipelines for each of the models with full features list
    @fitBasicList - list with RF models trained pipelines for each of the models with basic features list
    @mlflow_params - dictionary with training parameters for mlflow
    @mlflow_params_extra - dictionary with extra training parameters for mlflow, if @checks=True
Example:
    pipelinePrepList, pipelinePrepBasicList, fitList, fitBasicList, mlflow_params, mlflow_params_extra = demand_forecast_training(data=data,
                                                col_target="revenue_tickets",
                                                first_pred_day='2019-01-15',
                                                jarra='mass',
                                                verbose=True,
                                                checks=True,
                                                logger=logger)
    Prepares data already partially processed in demand_forecast_data_prep() step, trains model using data until @first_pred_day-1 for @col_target,
    returns data prep and model pipelines, as well as saves them in S3. @data and @logger from demand_forecast_data_prep() step
"""

from lib.add_error_cols import add_error_cols
from lib.calc_training_set_error import calc_training_set_error
from lib.create_spark_session import create_spark_session
from lib.create_model_data_lists import create_training_data_lists
from lib.define_features import define_features
import os
from ConfigParser import SafeConfigParser
from datetime import datetime
from pyspark.sql.functions import col
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import OneHotEncoderEstimator, OneHotEncoderModel, OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor as RFRegr, RandomForestRegressionModel
import pdb
#from __init__ import get_data
#MODEL_CONFIG_FILE=get_data("config/demand_forecast_config.conf")
local_path=os.getcwd() + '/'
MODEL_CONFIG_FILE=local_path+'data/config/demand_forecast_config.conf'
def demand_forecast_training(data=False,
                             col_target="revenue_tickets",
                             first_pred_day=False,
                             jarra='quinto',
                             verbose=True,
                             checks=False,
                             logger=False):
    try:
        parser = SafeConfigParser()
        config_file_path = MODEL_CONFIG_FILE
        parser.read(config_file_path)
        cols_id = [e.strip() for e in parser.get('col_names', 'cols_id').split(',')]

        #model parameters (RF)
        min_samples_leaf = int(parser.get('random_forest_params', 'min_samples_leaf'))
        max_features = parser.get('random_forest_params', 'max_features')
        max_features = max_features if max_features == 'sqrt' else str(max_features)
        num_trees = int(parser.get('random_forest_params', 'num_trees'))
        max_depth = int(parser.get('random_forest_params', 'max_depth'))
        subsampling_rate = float(parser.get('random_forest_params', 'subsampling_rate'))
        #other parameters from parser
        training_sample = float(parser.get('random_forest_params', 'training_sample'))
        model_complexity = parser.get('model_params', 'model_complexity')
        use_clustered_data_sets = parser.getboolean('model_params', 'use_clustered_data_sets')
        cols_id = [e.strip() for e in parser.get('col_names', 'cols_id').split(',')]
        col_predict = col_target + '_pred'
        #set 1st day of predictions
        if first_pred_day is not None:
            split_value = first_pred_day
        else:
            split_value = datetime.today()
            first_pred_day = split_value.strftime('%Y-%m-%d')
            split_value = split_value.strftime('%Y-%m-%d')
        #save parameters
        s3_save_path = parser.get('save_params', 's3_save_path')
        s3_save_pipelines_path = s3_save_path + 'pipelines/'+col_target+'/dt-execution=' + split_value + '/'
        #connect to spark
        spark = create_spark_session(jarra=jarra,
                                     verbose=verbose,
                                     logger=logger)
        #define features for each of the models (we have separate models for next 7 days, days 8-14, 15-32 and so on), also features for basic model (for flights w/o historical data)
        features_list, features_list_basic_model, ohe_variables_in, cyclical_variables =\
        define_features(model_complex=model_complexity,
                        use_clustered_data_sets=use_clustered_data_sets,
                        col_target=col_target,
                        verbose=verbose,
                        logger=logger)
        ohe_variables_out = [s + '_catVec' for s in ohe_variables_in]
        cyclical_variables_sin = [s + '_sin' for s in cyclical_variables]
        cyclical_variables_cos = [s + '_cos' for s in cyclical_variables]
        cyclical_variables_out = cyclical_variables_sin+cyclical_variables_cos
        #add cyclical variables to features lists
        for i in range(len(features_list)):
            features_list[i] = features_list[i] + cyclical_variables_out
            features_list_basic_model[i] = features_list_basic_model[i] + cyclical_variables_out
        if verbose:
            logger.info(ohe_variables_in)
            logger.info('Number of partition of data df: ' + str(data.rdd.getNumPartitions()))
        #define date filters for training set and test/pred sets of each consecutive models
        filterTrainEndList = []
        for i in range(len(features_list)):
            filterTrainEndList.append(col('dt_flight_date_local') < split_value)
        #create list with data sets for each of the consecutive models, each data set have different features
        #split on date is done later in the script.
        #also data list for rows/flights with Nulls (e.g. no historical data) is created separately
        data = data.coalesce(200)
        train_data_list, train_data_basic_list = create_training_data_lists(data,
                                                                             features_list,
                                                                             features_list_basic_model,
                                                                             ohe_variables_in,
                                                                             col_target,
                                                                             cols_id,
                                                                             filterTrainEndList,
                                                                             use_clustered_data_sets,
                                                                             training_sample,
                                                                             spark,
                                                                             verbose,
                                                                             logger)
        #ad-hoc, there are some nans in hour (should not be in the new table ds_flights), remove in future
        for i in range(len(train_data_list)):
            train_data_list[i] = train_data_list[i].dropna(how='any', subset=['dt_flight_hour_local_zone_sin'])
            train_data_basic_list[i] = train_data_basic_list[i].dropna(how='any', subset=['dt_flight_hour_local_zone_sin'])
        #string indexer, one hot encoder and vector assembler (creates column 'features' with all the features for given model as vector),
        #and create pipelines for each consecturive model (also 'basic' models)
        if verbose:
            logger.info('String indexer, one hot encoder and vector assembler, start')
        #indexer: transforms string values into numeric values, the value that occurs in data most is indexed as zero, second as 1 etc.
        indexers = [StringIndexer(inputCol=x,
                                  outputCol=x+'_tmp',
                                  handleInvalid='keep')
                    for x in ohe_variables_in]
        #one hot encoding
        ohe_variables_in_tmp = [i + '_tmp' for i in ohe_variables_in]
        encoder = OneHotEncoderEstimator(dropLast=True,
                                         inputCols=ohe_variables_in_tmp,
                                         outputCols=ohe_variables_out,
                                         handleInvalid ='keep')
        #add to pipeline
        pipeline_tmp=indexers + [encoder]
        #create placeholders
        assembler = []
        pipeline_tmp2 = []
        pipeline_list = []
        pipelinePrepList = []
        trainDataList = []
        assembler_basic = []
        pipeline_basic_tmp2 = []
        pipeline_basic_list = []
        pipelinePrepBasicList = []
        trainDataBasicList = []
        start = datetime.now()
        for i in range(len(train_data_list)):
            if verbose:
                logger.info('Model ' + str(i))
            features_list[i] = features_list[i] + ohe_variables_out
            assembler.append(VectorAssembler(inputCols=features_list[i],
                                             outputCol='features'))
            pipeline_tmp2.append(pipeline_tmp + [assembler[i]])
            pipeline_list.append(Pipeline(stages=pipeline_tmp2[i]))
            pipelinePrepList.append(pipeline_list[i].fit(train_data_list[i]))
            trainDataList.append(pipelinePrepList[i].transform(train_data_list[i]))

            #the same for basic model
            features_list_basic_model[i] = features_list_basic_model[i] + ohe_variables_out
            assembler_basic.append(VectorAssembler(inputCols=features_list_basic_model[i],
                                                   outputCol='features'))
            pipeline_basic_tmp2.append(pipeline_tmp + [assembler_basic[i]])
            pipeline_basic_list.append(Pipeline(stages=pipeline_basic_tmp2[i]))
            pipelinePrepBasicList.append(pipeline_basic_list[i].fit(train_data_basic_list[i]))
            trainDataBasicList.append(pipelinePrepBasicList[i].transform(train_data_basic_list[i]))

        end = datetime.now()

        if verbose:
            logger.info('First day of model training set: ' + str(trainDataList[0].toPandas().dt_flight_date_local.min()))
            logger.info('Last day of model training set: ' + str(trainDataList[0].toPandas().dt_flight_date_local.max()))
            logger.info('String indexer, one hot encoder and vector assembler, done for all models, time: ' + str(end-start))
            logger.info('Number of partition of trainDataList 0 df: ' + str(trainDataList[0].rdd.getNumPartitions()))
            logger.info('Number of partition of trainDataList 5 df: ' + str(trainDataList[5].rdd.getNumPartitions()))
            logger.info('Features list for first model (ie. for next 7 days): ')
            logger.info(features_list[0])
            logger.info('RF Model start')

        start_all = datetime.now()
        #create placeholders
        RFModelList = []
        fitList = []
        RFModelBasicList = []
        fitBasicList = []
        for i in range(len(trainDataList)):
            start = datetime.now()
            if verbose:
                logger.info('Model '+str(i))
            RFModelList.append(RFRegr(labelCol=col_target,
                           featuresCol='features',
                           numTrees=num_trees,
                           maxDepth=max_depth,
                           featureSubsetStrategy=max_features,
                           subsamplingRate=subsampling_rate,
                           minInstancesPerNode=min_samples_leaf
                           ))
            RFModelBasicList.append(RFRegr(labelCol=col_target,
                           featuresCol='features',
                           numTrees=num_trees,
                           maxDepth=max_depth,
                           featureSubsetStrategy=max_features,
                           subsamplingRate=subsampling_rate,
                           minInstancesPerNode=min_samples_leaf
                           ))
            #repartition to get the evenly distributed data set:
            trainDataList[i] = trainDataList[i].coalesce(36)
            trainDataBasicList[i] = trainDataBasicList[i].coalesce(36)
            #fit to training set
            fitList.append(RFModelList[i].setPredictionCol(col_predict).fit(trainDataList[i]))
            #the same for 'basic' models
            fitBasicList.append(RFModelBasicList[i].setPredictionCol(col_predict).fit(trainDataBasicList[i]))
            end = datetime.now()
            if verbose:
                logger.info('Random Forest, ' + str(i) + ' model, time: ' + str(end-start))
        if verbose:
            logger.info('Saving data preparation and model pipelines in ' + s3_save_pipelines_path)
        for i in range(len(pipelinePrepList)):
            pipelinePrepList[i].write().overwrite().save(s3_save_pipelines_path+"data_prep_pipeline"+str(i))
            pipelinePrepBasicList[i].write().overwrite().save(s3_save_pipelines_path+"data_prep_basic_pipeline"+str(i))
            fitList[i].write().overwrite().save(s3_save_pipelines_path+"model_pipeline"+str(i))
            fitBasicList[i].write().overwrite().save(s3_save_pipelines_path+"model_basic_pipeline"+str(i))
        if checks:
            mlflow_params_extra = calc_training_set_error(fitList,
                                                          fitBasicList,
                                                          trainDataList,
                                                          trainDataBasicList,
                                                          cols_id,
                                                          col_predict,
                                                          col_target,
                                                          verbose,
                                                          logger)
        else:
            mlflow_params_extra = {}

        end_all = datetime.now()
        if verbose:
            logger.info('Random Forest, all models, time: ' + str(end_all-start_all))
        max_features_int = max_features if max_features == 'sqrt' else float(max_features)
        mlflow_params = {'col_target': col_target,
                         'num_trees': num_trees,
                         'max_depth': max_depth,
                         'max_features': max_features_int,
                         'subsampling_rate': subsampling_rate,
                         'min_samples_leaf': min_samples_leaf,
                         'training_sample': training_sample,
                         'train_date_min': str(trainDataList[0].toPandas().dt_flight_date_local.min()),
                         'train_date_max': str(trainDataList[5].toPandas().dt_flight_date_local.max()),
                         'time_seconds': (end_all-start_all).total_seconds()
                         }
        #spark.stop()
        #if verbose:
        #    logger.info('Spark Session stopped')
    except Exception:
        logger.exception("Fatal error in demand_forecast_training()")
        raise
    return(pipelinePrepList, pipelinePrepBasicList, fitList, fitBasicList, mlflow_params, mlflow_params_extra)
