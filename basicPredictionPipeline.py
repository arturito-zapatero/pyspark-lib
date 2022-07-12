"""
Created on 20.11.2018
@author: aszewczyk
Function that applies the data prep and model pipelines from S3 (saved there in model_train() step) to new data to predict @col_target on flight level until
the end of the next next month
Input:
    @data - spark d.f. with model data prepared in demand_forecast_data_prep() step (we apply here date filters to get dates we need depending on first_pred_day)
    @col_target - string with target variable (pax_seat, revenue_tickets or ancis)
    @first_pred_day - first day of predictions, if not given today is set, model predictions are made from this day to end of next next month
    @dt_execution - use data prep and model pipelines created by model_train() step run on this day and saved in S3, if False first_pred_day is used
    @jarra - string with size of the cluster on AWS ('quinto', 'tercio', 'mediana', 'mass')
    @logger - logger connection opened in demand_forecast_data_prep() step
    @verbose - should print logger messages on the screen and save them to .log file?
    @checks - should calculate advanced metrics?
Returns:
    @mlflow_params - dictionary with prediction step parameters for mlflow
    @pred_errors - evaluated metrics of past models
Example:
    mlflow_params, pred_errors = demand_forecast_pred(data=data,
                                     col_target='revenue_tickets',
                                     first_pred_day='2018-01-15',
                                     dt_execution='2017-01-01',
                                     jarra='quinto',
                                     logger=logger,
                                     verbose=True,
                                     checks=False)
    Creates prediction for revenue from tickets from 2018-01-15 until 2018-03-31 using model pipelines trained on 2017-01-01, @data and @logger from 
    demand_forecast_data_prep() step
TODO: general refactoring
"""

from lib.calcFeatImportance import calcFeatImportance
from lib.calcTrainingSetError import calcTrainingSetError
from lib.createTestDatesListWFV import createTestDatesListWFV
from lib.createSparkSession import createSparkSession
from lib.createTestDataLists import createTestDataLists
from lib.defineFeatures import defineFeatures

from ConfigParser import SafeConfigParser
from datetime import datetime
import numpy as np
import os
from pyspark.sql.functions import col, sum, to_date, lit, count
from pyspark.ml import PipelineModel
from pyspark.ml.regression import RandomForestRegressionModel

local_path=os.getcwd() + '/'
MODEL_CONFIG_FILE=local_path+'data/config/file_name.conf'


def basicPredictionPipeline(
    data,
    col_target: str = "",
    first_pred_day: [str, bool] = False,
    dt_execution: [str, bool] = False,
    jarra: str = 'quinto',
    logger: bool = False,
    verbose: bool = True,
    checks: bool = False
):
    try:
        
        start_all = datetime.now()

        # Get parameters from config file
        number_of_models = 6
        parser = SafeConfigParser()
        parser.read(MODEL_CONFIG_FILE)
        local_save_path = parser.get('save_params', 'local_save_path')
        if not os.path.exists(local_save_path):
            os.makedirs(local_save_path)
        local_save_path = parser.get('save_params', 'local_save_path')

        # Define name of the variable for predictions
        cols_cyclical, cols_ohe_in, cols_features, col_target, cols_id = defineFeatures(model_complex='first',
                                                                                        use_clustered_data_sets=False,
                                                                                        col_target=col_target,
                                                                                        verbose=False,
                                                                                        logger=False)

        cols_ohe_out = [s + '_catVec' for s in cols_ohe_in]
        if first_pred_day is not None:
            split_value = first_pred_day
        else:
            split_value = datetime.today()
            first_pred_day = split_value.strftime('%Y-%m-%d')
            split_value = split_value.strftime('%Y-%m-%d')
        if not dt_execution:
            dt_execution = split_value
        s3_save_path = parser.get('save_params', 's3_save_path')
        s3_save_pipelines_path = s3_save_path + 'pipelines/'+col_target+'/dt-execution=' + dt_execution + '/'

        # Connect to spark session
        spark = createSparkSession(jarra='mass',
                                     verbose=True,
                                     logger=logger)

        # Load data prep and model pipelines from S3 for model training run on dt_execution:
        if verbose:
            logger.info('Loading data preparation and model pipelines lists from ' + s3_save_pipelines_path)

        pipelinePrepList = []
        fitList = []
        for i in range(number_of_models):
            pipelinePrepList.append(PipelineModel.read().load(s3_save_pipelines_path+"data_prep_pipeline"+str(i)))
            fitList.append(RandomForestRegressionModel.read().load(s3_save_pipelines_path+"model_pipeline"+str(i)))
        if verbose:
            logger.info('Loading data preparation and model pipelines lists end')

        # Add cyclical variables to features lists, OHE_out not as they are already in pipelines
        cols_cyclical_sin = [s + '_sin' for s in cols_cyclical]
        cols_cyclical_cos = [s + '_cos' for s in cols_cyclical]
        cols_cyclical_out = cols_cyclical_sin + cols_cyclical_cos
        
        for i in range(len(cols_features)):
            cols_features[i] = cols_features[i] + cols_cyclical_out

        # Create list with start and end dates for each of consecutive models
        start_days_list, end_days_list = createTestDatesListWFV(split_value,
                                                                verbose=verbose,
                                                                logger=logger
                                                                )

        # Define date filters for test/pred sets of each consecutive models
        filterPredStartList = []
        filterPredEndList = []
        for i in range(len(start_days_list)):
            filterPredStartList.append(col('dt_flight_date_local') >= start_days_list[i])
            filterPredEndList.append(col('dt_flight_date_local') <= end_days_list[i])

        # Create list with test data sets for each of the consecutive models, each data set have different features
        # and dates, also data list for rows/flights with Nulls (e.g. no historical data) is created separately
        test_data_list, test_data_basic_list = createTestDataLists(data,
                                                                   cols_features,
                                                                   cols_ohe_in,
                                                                   col_target,
                                                                   cols_id,
                                                                   filterPredStartList,
                                                                   filterPredEndList,
                                                                   spark,
                                                                   verbose,
                                                                   logger)

        # Transform string idexer, ohe, vector assembler using pipeline from training
        if verbose:
            logger.info('String indexer, one hot encoder and vector assembler test sets, start')

        testDataList = []
        testDataBasicList = []
        for i in range(len(test_data_list)):
            if verbose:
                logger.info('Model ' + str(i))
            testDataList.append(pipelinePrepList[i].transform(test_data_list[i]))
        if verbose:
            logger.info('RF Model start')

        # Apply RF model data using pipeline from training
        resultsList = []
        resultsBasicList = []
        for i in range(len(testDataList)):
            # Use the test set, is creating an extra column 'col_target' with the test fit results
            resultsList.append(fitList[i].transform(testDataList[i]).select(cols_id + [col_target + '_pred']))
        if verbose:
            logger.info('RF Model end')

        # Union dataframes with results for each model as one dataframe (to get the full results)
        resultsFull = resultsList[0]
        resultsFull = resultsFull.union(resultsBasicList[0])
        for i in range(1,len(test_data_list)):
            resultsFull = resultsFull.union(resultsList[i])
            resultsFull = resultsFull.union(resultsBasicList[i])
        resultsFull.cache()
        resultsFull = resultsFull.withColumn('dt_flight_date_local' ,to_date('dt_flight_date_local'))

        # Add execution date column
        resultsFull = resultsFull.withColumn('dt_execution', lit(first_pred_day))
        resultsFull = resultsFull.withColumn('dt_execution' ,to_date('dt_execution'))

        # Save prediction results in local for each model seperately
        if verbose:
            logger.info('Changing data frame to Pandas to save in local')
        model_results = resultsFull.toPandas()
        if not os.path.isdir(local_save_path):
            os.mkdir(local_save_path)
        model_results\
        .to_csv(local_save_path + col_target + '_results_' + first_pred_day.replace('-', '_') + '.csv', index=False)
        if verbose:
            logger.info('Results saved in: ' + local_save_path + col_target + '_results_' +
                        first_pred_day.replace('-', '_') + '.csv')

        # Get feature importances
        featureImportancesFirst, featureImportancesLast, feature_importances_all = calcFeatImportance(fitList,
                                                                                                      testDataList,
                                                                                                      col_target,
                                                                                                      first_pred_day,
                                                                                                      verbose,
                                                                                                      logger)

        # Save feature importance for given target variable
        feature_importances_all.\
        to_csv(local_save_path + col_target + '_feat_importance_' +
               first_pred_day.replace('-', '_') + '.csv', index=False)
        end_all = datetime.now()
        if verbose:
            logger.info('Random Forest, all models, time: ' + str(end_all-start_all))   
            logger.info('Feature importance saved in: ' + local_save_path + col_target + '_feat_importance_' +
                        first_pred_day.replace('-', '_') + '.csv')
            logger.info('Check sum of predicted variables per month and count of flights each month: ')

        # Calculate metrics for mlflow
        if verbose and checks:
            df_prediction_errors, pred_errors = calcTrainingSetError(number_of_last_days_to_eval=90,
                          last_dt_exec_to_evaluate=False,
                          list_exec_dates_to_evalute=False,
                          remove_outliers=True,
                          verbose=True,
                          logger=logger,
                          checks=True
                          )

            checkDuplicates = resultsFull.drop_duplicates(subset=['dt_flight_date_local',
                                                                  'cd_num_flight',
                                                                  'cd_airport_pair',
                                                                  'cd_carrier'])\
                              .count() - resultsFull.count()
            resultsFullCount = resultsFull.count()

            # Count sum of rows in all test sets
            testSetCount = np.sum([testDataList[i].count() for i in range(len(testDataList))])
            testBasicSetCount = np.sum([testDataBasicList[i].count() for i in range(len(testDataBasicList))])

            logger.info('Sum of flights per month (real values): ')
            logger.info(resultsFull.groupBy("dt_flight_year_month")
                        .agg(count("cd_airport_pair"))
                        .sort("dt_flight_year_month")
                        .toPandas())
            logger.info('Sum of predicted ' + col_predict + ' per month (all flights): ')
            logger.info(resultsFull.groupBy("dt_flight_year_month")
                        .agg(sum(col_predict))
                        .sort("dt_flight_year_month")
                        .toPandas())
            logger.info('Number of duplicated flights: ')

            logger.info('Number of rows/flights in test sets: ' + str(testSetCount))
            logger.info('Number of rows/flights in basic model test sets: ' + str(testBasicSetCount))
            logger.info('Number of flights/rows in prediction set:')
            logger.info(resultsFullCount)
            logger.info('Feature importances for the first model (flights this week):')
            logger.info(featureImportancesFirst)
            logger.info('Feature importances for the last model:')
            logger.info(featureImportancesLast)
            mlflow_params = {'checkDuplicates': checkDuplicates, 
                             'resultsFullCount': resultsFullCount,
                             'testSetCount': testSetCount,
                             'testBasicSetCount': testBasicSetCount,
                             'predDateMin': str(resultsFull.toPandas().dt_flight_date_local.min()), 
                             'predDateMax': str(resultsFull.toPandas().dt_flight_date_local.max()),
                             'time_seconds': (end_all-start_all).total_seconds()
                            }
        else:
            mlflow_params = {}

    except Exception:
        logger.exception("Fatal error in demand_forecast_pred()")
        raise

    return(mlflow_params, pred_errors)
