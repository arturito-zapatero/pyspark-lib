"""
Created on 20.11.2018
@author: aszewczyk
Function that prepares (e.g. OHE) data already partially processed in demand_forecast_data_prep() step, trains model
using data until @first_pred_day-1 for @col_target, returns data prep and model pipelines, as well as saves them in S3
Input:
    @data - spark d.f. with model data prepared in demand_forecast_data_prep() step (we apply here date filters to get
     dates we need depending on first_pred_day)
    @col_target - string with target variable (pax_seat, revenue_tickets or ancis)
    @first_pred_day - last day-1 of data used for training, if not given today is set
    @jarra - string with size of the cluster on AWS ('quinto', 'tercio', 'mediana', 'mass')
    @logger - logger connection opened in demand_forecast_data_prep() step
    @verbose - should print logger messages on the screen and save them to .log file?
    @checks - should calculate advanced metrics, @mlflow_params_extra?
Returns:
    @pipelinePrepList - list with data preparation pipelines (StringIndexer, OHE etc.) for each of the models with full
    features list
    @pipelinePrepBasicList - list with data preparation pipelines (StringIndexer, OHE etc.) for each of the models with
     basic features list
    @fitList - list with RF models trained pipelines for each of the models with full features list
    @fitBasicList - list with RF models trained pipelines for each of the models with basic features list
    @mlflow_params - dictionary with training parameters for mlflow
    @mlflow_params_extra - dictionary with extra training parameters for mlflow, if @checks=True
Example:
    pipelinePrepList, pipelinePrepBasicList, fitList, fitBasicList, mlflow_params, mlflow_params_extra =
    demand_forecast_training(data=data,
                            col_target="revenue",
                            first_pred_day='2019-01-15',
                            jarra='mass',
                            verbose=True,
                            checks=True,
                            logger=logger)
    Prepares data already partially processed in demand_forecast_data_prep() step, trains model using data until
    @first_pred_day-1 for @col_target,
    returns data prep and model pipelines, as well as saves them in S3. @data and @logger from
    demand_forecast_data_prep() step
TODO: refactor!
"""

from lib.addErrorCols import addErrorCols
from lib.calcTrainingSetError import calcTrainingSetError
from lib.createSparkSession import createSparkSession
from lib.createTrainingDataLists import createTrainingDataLists
from lib.defineFeatures import defineFeatures
import os
from ConfigParser import SafeConfigParser
from datetime import datetime
from pyspark.sql.functions import col
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import OneHotEncoderEstimator, OneHotEncoderModel, OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor as RFRegr, RandomForestRegressionModel

local_path=os.getcwd() + '/'
MODEL_CONFIG_FILE=local_path+'data/config/file_name.conf'


def basicTrainingPipeline(
    data,
    col_target: str,
    first_pred_day: [bool, str] = False,
    jarra: str = 'quinto',
    verbose: bool = True,
    checks: bool = False,
    logger: bool = False
):
    try:
        parser = SafeConfigParser()
        config_file_path = MODEL_CONFIG_FILE
        parser.read(config_file_path)

        # Model parameters (RF)
        min_samples_leaf = int(parser.get('random_forest_params', 'min_samples_leaf'))
        max_features = parser.get('random_forest_params', 'max_features')
        max_features = max_features if max_features == 'sqrt' else str(max_features)
        num_trees = int(parser.get('random_forest_params', 'num_trees'))
        max_depth = int(parser.get('random_forest_params', 'max_depth'))
        subsampling_rate = float(parser.get('random_forest_params', 'subsampling_rate'))

        # Other parameters from parser
        training_sample = float(parser.get('random_forest_params', 'training_sample'))
        model_complexity = parser.get('model_params', 'model_complexity')
        use_clustered_data_sets = parser.getboolean('model_params', 'use_clustered_data_sets')
        cols_id = [e.strip() for e in parser.get('col_names', 'cols_id').split(',')]
        col_predict = col_target + '_pred'

        # Set 1st day of predictions
        if first_pred_day is not None:
            split_value = first_pred_day
        else:
            split_value = datetime.today()
            first_pred_day = split_value.strftime('%Y-%m-%d')
            split_value = split_value.strftime('%Y-%m-%d')

        # Save parameters
        s3_save_path = parser.get('save_params', 's3_save_path')
        s3_save_pipelines_path = s3_save_path + 'pipelines/'+col_target+'/dt-execution=' + split_value + '/'

        # Connect to spark
        spark = createSparkSession(jarra=jarra,
                                     verbose=verbose,
                                     logger=logger)

        # Define name of the variable for predictions
        cols_cyclical, cols_ohe_in, cols_features, col_target, cols_id = defineFeatures(model_complex='first',
                                                                                        use_clustered_data_sets=False,
                                                                                        col_target=col_target,
                                                                                        verbose=False,
                                                                                        logger=False)

        # Add cyclical variables to features lists, OHE_out not as they are already in pipelines
        cols_cyclical_sin = [s + '_sin' for s in cols_cyclical]
        cols_cyclical_cos = [s + '_cos' for s in cols_cyclical]
        cols_cyclical_out = cols_cyclical_sin + cols_cyclical_cos
        for i in range(len(cols_features)):
            cols_features[i] = cols_features[i] + cols_cyclical_out

        # Fill with features (depending on how many models we have)
        cols_ohe_out = []
        features_list = []

        col_date = ''
        if verbose:
            logger.info(cols_ohe_in)
            logger.info('Number of partition of data df: ' + str(data.rdd.getNumPartitions()))

        # Define date filters for training set and test/pred sets of each consecutive models
        filterTrainEndList = []
        for i in range(len(cols_features)):
            filterTrainEndList.append(col(col_date) < split_value)

        # Create list with data sets for each of the consecutive models, each data set have different features
        data = data.coalesce(200)
        train_data_list, train_data_basic_list = createTrainingDataLists(data,
                                                                         cols_features,
                                                                         cols_ohe_in,
                                                                         col_target,
                                                                         cols_id,
                                                                         filterTrainEndList,
                                                                         use_clustered_data_sets,
                                                                         training_sample,
                                                                         spark,
                                                                         verbose,
                                                                         logger)

        # String indexer, one hot encoder and vector assembler (creates column 'features' with all the features for
        # given model as vector),
        if verbose:
            logger.info('String indexer, one hot encoder and vector assembler, start')

        # Indexer: transforms string values into numeric values, the value that occurs in data most is indexed as zero,
        # second as 1 etc.
        indexers = [StringIndexer(inputCol=x,
                                  outputCol=x+'_tmp',
                                  handleInvalid='keep')
                    for x in cols_ohe_in]

        # One hot encoding
        cols_ohe_in_tmp = [i + '_tmp' for i in cols_ohe_in]
        encoder = OneHotEncoderEstimator(dropLast=True,
                                         inputCols=cols_ohe_in_tmp,
                                         outputCols=cols_ohe_out,
                                         handleInvalid ='keep')


        # Add to pipeline
        pipeline_tmp=indexers + [encoder]

        # Create placeholders
        assembler = []
        pipeline_tmp2 = []
        pipeline_list = []
        pipelinePrepList = []
        trainDataList = []
        pipelinePrepBasicList = []
        trainDataBasicList = []
        start = datetime.now()
        for i in range(len(train_data_list)):
            if verbose:
                logger.info('Model ' + str(i))
            features_list[i] = features_list[i] + cols_ohe_out
            assembler.append(VectorAssembler(inputCols=features_list[i],
                                             outputCol='features'))
            pipeline_tmp2.append(pipeline_tmp + [assembler[i]])
            pipeline_list.append(Pipeline(stages=pipeline_tmp2[i]))
            pipelinePrepList.append(pipeline_list[i].fit(train_data_list[i]))
            trainDataList.append(pipelinePrepList[i].transform(train_data_list[i]))

        end = datetime.now()

        if verbose:
            logger.info('First day of model training set: ' +
                        str(trainDataList[0].toPandas().dt_flight_date_local.min()))
            logger.info('Last day of model training set: ' +
                        str(trainDataList[0].toPandas().dt_flight_date_local.max()))
            logger.info('String indexer, one hot encoder and vector assembler, done for all models, time: ' +
                        str(end-start))
            logger.info('Number of partition of trainDataList 0 df: ' + str(trainDataList[0].rdd.getNumPartitions()))
            logger.info('Number of partition of trainDataList 5 df: ' + str(trainDataList[5].rdd.getNumPartitions()))
            logger.info('Features list for first model (ie. for next 7 days): ')
            logger.info(features_list[0])
            logger.info('RF Model start')

        start_all = datetime.now()

        # Create placeholders
        RFModelList = []
        fitList = []
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

            # Repartition to get the evenly distributed data set:
            trainDataList[i] = trainDataList[i].coalesce(36)

            # Fit to training set
            fitList.append(RFModelList[i].setPredictionCol(col_predict).fit(trainDataList[i]))
            end = datetime.now()
            if verbose:
                logger.info('Random Forest, ' + str(i) + ' model, time: ' + str(end-start))
        if verbose:
            logger.info('Saving data preparation and model pipelines in ' + s3_save_pipelines_path)
        for i in range(len(pipelinePrepList)):
            pipelinePrepList[i].write().overwrite().save(s3_save_pipelines_path+"data_prep_pipeline"+str(i))
            fitList[i].write().overwrite().save(s3_save_pipelines_path+"model_pipeline"+str(i))

        if checks:
            mlflow_params_extra = calcTrainingSetError(fitList,
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

    except Exception:
        logger.exception("Fatal error in demand_forecast_training()")
        raise

    return pipelinePrepList, fitList, mlflow_params, mlflow_params_extra
