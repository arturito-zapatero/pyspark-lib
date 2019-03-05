# -*- coding: utf-8 -*-
"""
Created in 2018
@author: aszewczyk
Creates and evaluates the random forest model using walk forward validation.
	Several model are created for consecutive evaluation periods in the future (ie. one for next 7 days, next for days 8-14 and so forth).
	For evaluation of different hyperparameters three for loops are used. This can be changed by adding more for loops.
Input:
		@data_list - list with pandas dfs with data for consecutive models (features, target, id columns)
		@X_train_list - list with pandas dfs with training feature columns
		@y_train_list - list with pandas dfs with training target column
		@X_test_list - list with pandas dfs with test feature columns
		@y_test_list - list with pandas dfs with test target column, also here the test set evaluation will be saved
		@y_train_list_results - copy of list with pandas dfs with training target column to save training set evaluation
		@col_predict - string with name of column to save the predictions made on test set
		@col_train_predict - string with name of column to save the predictions made on training set
		@cols_features_7d - list with string denoting column names of the model for next 7 days 
		@col_target - string with name of the target column
		@cols_id - list with strings with names of ID columns, ie. the columns necessary to identify given row of data - flight, this cols are saved together with the results
		@split_value - datetime object denoting date where the test set starts (ie. training sets ends one day before)
		@first_day_data - denotes date where the training set starts (string of format 'YYYY-mm-dd')
		@minSamplesLeafList - list with hyperparameters min samples leaf to be evaluated
		@maxFeaturesList - list with hyperparameters max features to be evaluated
		@numTreesList - list with hyperparameters num trees to be evaluated
		@n_jobs - how many processors should model evaluation use 
		@random_state
		@model_evaluation - 'basic' (evaluates only most important metrics like APE and MAPE) or 'full' model evaluation (evaluate more metrics, includes variable importance as         well)
		@spark - spark session
		@verbose - should print logger messages on the screen and save them to .log file?
		@logger - logger connection
Returns:
    @model_results - pandas data frame with saved model results
		@model_results_spk - spark data frame with saved model results
	"""
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from demand_forecast_model_eval import demand_forecast_model_eval
#from lib.demand_forecast_model_eval import demand_forecast_model_eval as lalala
def rf_model_eval(data_list,
                   X_train_list,
                   y_train_list,
                   X_test_list,
                   y_test_list,
                   y_train_list_results,
                   col_predict,
                   col_train_predict,
                   cols_features_7d, 
                   col_target,
                   cols_id,
                   split_value,
                   first_day_data,
                   minSamplesLeafList,
                   maxFeaturesList,
                   numTreesList,
                   max_depth,
                   n_jobs,
                   random_state,
                   model_evaluation,
                   spark,
                   verbose,
                   logger
                   ):
    try:
        if verbose:
            logger.info('Random Forest walk forward forward validation start, function rf_model_eval()')
            logger.info('First day of prediction: ' + split_value)
            logger.info('Prediction until the last day of the next next month')
            logger.info('Full features list: ' + str(cols_features_7d))
            logger.info('Target variable: ' + str(col_target))
            logger.info('Comments: none')
        #start model evaluation for every combination of hyperparameters (using loops) 
        for i_value in minSamplesLeafList:
            for j_value in maxFeaturesList:
                for k_value in numTreesList:
                    #create data frame to save the results
                    model_results = pd.DataFrame()
                    model_results_train = pd.DataFrame()
                    #here additional loop for consecutive data sets/models in future
                    for model in range(len(data_list)):
                        if verbose:
                            logger.info('Start of model' + str(model))
                        #start = time.time()
                        #select model parameters from the list
                        min_samples_leaf = i_value
                        max_features = j_value
                        n_estimators = k_value
                        #description for logging
                        i_param = 'min_samples_leaf'
                        j_param = 'max_features'
                        k_param = 'n_estimators'

                        #define the model object, other possibility:  ExtraTreesRegressor 
                        rf_def_1m = RandomForestRegressor(n_estimators=n_estimators,
                                                          max_features=max_features, 
                                                          min_samples_leaf=min_samples_leaf,
                                                          max_depth=max_depth,
                                                          oob_score=False,
                                                          bootstrap=True,
                                                          random_state = random_state,
                                                          n_jobs=n_jobs)

                        if (len(X_train_list[model]) <> 0 and len(y_train_list[model]) <> 0 and
                            len(X_test_list[model])  <> 0 and len(y_test_list[model]) <> 0 and
                            len(y_train_list_results[model]) <> 0):
                            #train and predict model numer 'model' on training set
                            rf_def_1m.fit(X_train_list[model], y_train_list[model])
                            y_hats_train = rf_def_1m.predict(X_train_list[model])
                            #creates a df (model_results_train) with concatenated model predictions on training sets for all the models.
                            y_train_list_results[model][col_train_predict] = y_hats_train
                            data_list[model][col_train_predict] = np.nan
                            data_list[model].loc[y_train_list_results[model].index, col_train_predict] = y_train_list_results[model][col_train_predict]
                            model_results_train_tmp = data_list[model].loc[:, cols_id+[col_target]+[col_train_predict]].dropna(how='any')
                            model_results_train = pd.concat([model_results_train, model_results_train_tmp])
                            #predict on the test set using trained model
                            y_hats_1m = rf_def_1m.predict(X_test_list[model])
                            #creates a data frame with concatenated results of all the models on test sets (ie. results for each flight
                            #in the period from today up to end of the next next month). Each line is day-flight number (ie flight)
                            #and there should be no duplicates.
                            #The d.f. contains target column, prediction and id columns that identify the flight (like day and airport)
                            y_test_list[model][col_predict] = y_hats_1m
                            data_list[model][col_predict] = np.nan
                            data_list[model].loc[y_test_list[model].index, col_predict] = y_test_list[model][col_predict]
                            model_results_tmp = data_list[model].loc[:, cols_id+[col_target]+[col_predict]].dropna(how='any')
                            model_results = pd.concat([model_results, model_results_tmp])

                            #calculate variable importance for the first and the last model (ie. the ones with lowest and highest number of features)
                            if (model_evaluation == 'full' and verbose):
                                if model == 0:
                                    feature_importances_first = pd.DataFrame(rf_def_1m.feature_importances_,
                                                                        index = X_test_list[model].columns,
                                                                        columns=['importance']).sort_values('importance', 
                                                                               ascending=False)
                                    if verbose:
                                        logger.info('Feature importance for the 7 days model \n column' + str(feature_importances_first))
                                elif model == 5:

                                    feature_importances_last = pd.DataFrame(rf_def_1m.feature_importances_,
                                                                        index = X_test_list[model].columns,
                                                                        columns=['importance']).sort_values('importance', 
                                                                               ascending=False)
                                    if verbose:
                                        logger.info('Feature importance for 2 months model \n column' + str(feature_importances_last))
                            else:
                                feature_importances_first = None
                                feature_importances_last = None

                    #at this point all the models are evaluated for one set of hyperparameters. The results are gathered inside of one data frame. 
                    #It is time to evaluate the results for given set of hyperparameters.
                    if verbose:
                        logger.info('Used parameters are: \n subsamplingRate: ' +\
                        '\n ' + i_param + ': ' + str(i_value) +\
                        '\n ' + j_param + ': ' + str(j_value) +\
                        '\n ' + k_param + ': ' + str(k_value))
                    #evaluate the data and prints the metrics. evaluation is conducted in Pyspark, so we convert the df with results to spark df.
                    if not model_results_train.empty: 
                        if verbose:
                            logger.info('Evaluation using training set')
                        model_results_train_spk = spark.createDataFrame(model_results_train)
                        model_results_train_spk = demand_forecast_model_eval(model_results_train_spk,
                                                                             col_target,
                                                                             col_predict=col_train_predict,
                                                                             model_evaluation='basic',
                                                                             eval_start=first_day_data,
                                                                             verbose=verbose,
                                                                             logger=logger)
                    if not model_results.empty:
                        if verbose:
                            logger.info('Evaluation using test set')
                        model_results_spk = spark.createDataFrame(model_results)
                        model_results_spk = demand_forecast_model_eval(model_results_spk,
                                                                       col_target,
                                                                       col_predict,
                                                                       model_evaluation='basic',
                                                                       eval_start=split_value,
                                                                       verbose=verbose,
                                                                       logger=logger)
        #returns results for the last evaluated model
        if verbose:
            logger.info('Random Forest walk forward forward validation start')
    except Exception:
        logger.exception("Fatal error in rf_model_eval()")
        raise
    return(model_results, model_results_spk, feature_importances_first, feature_importances_last)
            
