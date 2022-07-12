"""
Created in Jan 2019
@author: aszewczyk
Function that avaluates the errors on training data set
Input:
    @fitList - list with model pipelines for each consecutive model with all features
    @fitBasicList - list with model pipelines for each consecutive model with basic features
    @trainDataList - list with df with model data for each consecutive model with all features
    @trainDataBasicList - list with df with model data for each consecutive model with basic features
    @cols_id - list with id columns that identify each row/flight
    @col_predict - string with prediction column name
    @col_target - string with target column name
    @verbose - should print logger messages on the screen and save them to .log file?
	@logger - logger connection
Returns:
    @transformedFull -  - spark df with model results with added error columns
"""
import logging

import pandas as pd
from lib.add_error_cols import add_error_cols


def calcTrainingSetError(
    fitList: list,
    fitBasicList: list,
    trainDataList: list,
    trainDataBasicList: list,
    cols_id: list,
    col_predict: str,
    col_target: str,
    verbose: bool,
    logger: logging.Logger
) -> dict:
    try:
        if verbose:
            logger.info('calc_training_set_error step start')

        # Fit to training set for training error evaluation for first and last models (ie. naxt week and in 3 months),
        # basic and full feature list
        resultsTrainFirstModel = fitList[0]\
                                    .transform(trainDataList[0])\
                                    .select(cols_id + [col_predict] + [col_target])
        resultsTrainLastModel  = fitList[5]\
                                    .transform(trainDataList[5])\
                                    .select(cols_id + [col_predict] + [col_target])
        resultsTrainFirstBasicModel = fitBasicList[0]\
                                    .transform(trainDataBasicList[0])\
                                    .select(cols_id + [col_predict] + [col_target])
        resultsTrainLastBasicModel  = fitBasicList[5]\
                                    .transform(trainDataBasicList[5])\
                                    .select(cols_id + [col_predict] + [col_target])

        # Add error columns
        resultsTrainFirstModel = add_error_cols(resultsTrainFirstModel,
                                                 col_target,
                                                 col_predict,
                                                 verbose,
                                                 logger)
        resultsTrainLastModel = add_error_cols(resultsTrainLastModel,
                                                 col_target,
                                                 col_predict,
                                                 verbose,
                                                 logger)
        resultsTrainFirstBasicModel = add_error_cols(resultsTrainFirstBasicModel,
                                                     col_target,
                                                     col_predict,
                                                     verbose,
                                                     logger)
        resultsTrainLastBasicModel = add_error_cols(resultsTrainLastBasicModel,
                                                     col_target,
                                                     col_predict,
                                                     verbose,
                                                     logger)

        # Convert to Pandas
        resultsTrainFirstModelPnd = resultsTrainFirstModel.toPandas()
        resultsTrainLastModelPnd = resultsTrainLastModel.toPandas()
        resultsTrainFirstBasicModelPnd = resultsTrainFirstBasicModel.toPandas()
        resultsTrainLastBasicModelPnd = resultsTrainLastBasicModel.toPandas()

        TrainMAPEFirstModel = resultsTrainFirstModelPnd[col_target + '_APE'].mean()
        TrainMAPELastModel = resultsTrainLastModelPnd[col_target + '_APE'].mean()
        TrainMAPEFirstBasicModel = resultsTrainFirstBasicModelPnd[col_target + '_APE'].mean()
        TrainMAPELastBasicModel = resultsTrainLastBasicModelPnd[col_target + '_APE'].mean()
        TrainMAEFirstModel = resultsTrainFirstModelPnd[col_target + '_AE'].mean()
        TrainMAELastModel = resultsTrainLastModelPnd[col_target + '_AE'].mean()
        TrainMAEFirstBasicModel = resultsTrainFirstBasicModelPnd[col_target + '_AE'].mean()
        TrainMAELastBasicModel = resultsTrainLastBasicModelPnd[col_target + '_AE'].mean()
        TrainCountFirstModel = resultsTrainFirstModelPnd.count()[0]
        TrainCountLastModel = resultsTrainLastModelPnd.count()[0]
        TrainCountFirstBasicModel = resultsTrainFirstBasicModelPnd.count()[0]
        TrainCountLastBasicModel = resultsTrainLastBasicModelPnd.count()[0]

        if verbose:
            logger.info('Training set MAPE for first full features model ' + col_target + ' is ' +
                              str(round(TrainMAPEFirstModel, 3)) + ' %')
            logger.info('Training set MAPE for last full features model ' + col_target + ' is ' +
                      str(round(TrainMAPELastModel, 3)) + ' %')
            logger.info('Training set MAPE for first basic features model ' + col_target + ' is ' +
                              str(round(TrainMAPEFirstBasicModel, 3)) + ' %')
            logger.info('Training set MAPE for last basic features model ' + col_target + ' is ' +
                      str(round(TrainMAPELastBasicModel, 3)) + ' %')
            logger.info('Training set MAPE for first full features model ' + col_target + ' is ' +
                              str(round(TrainMAEFirstModel, 3)))
            logger.info('Training set MAE for last full features model ' + col_target + ' is ' +
                      str(round(TrainMAELastModel, 3)))
            logger.info('Training set MAE for first basic features model ' + col_target + ' is ' +
                              str(round(TrainMAEFirstBasicModel, 3)))
            logger.info('Training set MAE for last basic features model ' + col_target + ' is ' +
                      str(round(TrainMAELastBasicModel, 3)))
            logger.info('Number of rows/flights in first full features model: ' + str(TrainCountFirstModel))
            logger.info('Number of rows/flights in last full features model: ' + str(TrainCountLastModel))
            logger.info('Number of rows/flights in first basic features model: ' + str(TrainCountFirstBasicModel))
            logger.info('Number of rows/flights in last basic features model: ' + str(TrainCountLastBasicModel))

        mlflow_params_extra = {'TrainMAPEFirstModel': TrainMAPEFirstModel,
            'TrainMAPELastModel': TrainMAPELastModel,
            'TrainMAPEFirstBasicModel': TrainMAPEFirstBasicModel,
            'TrainMAPELastBasicModel': TrainMAPELastBasicModel,
            'TrainMAEFirstModel': TrainMAEFirstModel,
            'TrainMAELastModel': TrainMAELastModel,
            'TrainMAEFirstBasicModel': TrainMAEFirstBasicModel,
            'TrainMAELastBasicModel': TrainMAELastBasicModel,
            'TrainCountFirstModel': TrainCountFirstModel,
            'TrainCountLastModel': TrainCountLastModel,
            'TrainCountFirstBasicModel': TrainCountFirstBasicModel,
            'TrainCountLastBasicModel': TrainCountLastBasicModel
            }
        if verbose:
            logger.info('calc_training_set_error step end')
    except Exception:
        logger.exception("Fatal error in calc_training_set_error()")
        raise

    return mlflow_params_extra
