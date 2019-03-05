# -*- coding: utf-8 -*-
"""
	TODOs: FUNCTION SHOULD BE CHANGED ACCORDINGLY TO RANDOM FOREST EVAL FUNCTION TO BE USED, dscriptions should be improved, log added
Created in 2018
@author: aszewczyk
Creates and evaluates the random forest model.
	Several model are created for consecutive evaluation periods in future (ie. one for next 7 days, next for days 8-14 and so forth)
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
		@cols_id - list with string with names of ID columns, ie. the columns necessary to identify given row of data - flight
		@split_value - datetime object denoting date where the test set starts (ie. training sets ends one day before)
		@first_day_data - denotes date where the training set starts (string of format 'YYYY-mm-dd')
		@minSamplesLeafList - list with hyperparameters min samples leaf for model evaluation
		@maxFeaturesList - list with hyperparameters max features for model evaluation
		@numTreesList - list with hyperparameters num trees for model evaluation
		@n_jobs - how many processors should model evaluation use 
		@random_state
		@model_evaluation - 'basic' (evaluates only most important metrics like APE and MAPE) or 'full' model evaluation (evaluate more metrics, includes variable importance as well)
		@spark - spark session
		@verbose - should print logger messages on the screen and save them to .log file?
		@logger - logger connection
Returns:
    @model_results - pandas data frame with saved model results
		@model_results_spk - spark data frame with saved model results
"""
import pandas as pd
import numpy as np
from xgboost import XGBRegressor
import demand_forecast_model_eval
def xgb_model_eval(data_list,
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
                   maxDepthList,
                   learningRateList,
                   numTreesList,
                   colsamplebyleveList,
                   subsampleList,
                   n_jobs,
                   random_state,
                   model_evaluation,
                   spark,
                   verbose,
                   logger
                   ):
    try:
        if verbose:
            logger.info('XGBoost walk forward forward validation start, function xgb_model_eval()')
            logger.info('Model training start')
            logger.info('Full features list: ' + str(cols_features_7d))
            logger.info('Target variable: ' + str(col_target))
            logger.info('Comments: baseline model')
        #start model evaluation for every combination of hyperparameters 
        for i_value in maxDepthList:
            for j_value in learningRateList:
                for k_value in numTreesList:
                    for l_value in colsamplebyleveList:
                        for m_value in subsampleList:
                            #create data frame to save the results
                            model_results = pd.DataFrame()
                            model_results_train = pd.DataFrame()
                            #here additional loop for each of the data sets/models
                            for model in range(len(data_list)):

                                print('Model' + str(model))
                                #start = time.time()
                                #select model parameters from the list
                                max_depth = i_value
                                learning_rate = j_value
                                n_estimators = k_value
                                colsample_bylevel = l_value
                                subsample = m_value

                                i_param = 'max_depth'
                                j_param = 'learning_rate'
                                k_param = 'number_of_trees'
                                l_param = 'colsample_bylevel'
                                m_param = 'subsample'
                                #define the model object
                                xgb_def = XGBRegressor(n_estimators=n_estimators,
                                       colsample_bylevel=colsample_bylevel, #Subsample ratio of columns for each split, in each level.
                                       colsample_bytree=1,  #Subsample ratio of columns when constructing each tree.
                                       subsample=subsample, #Subsample ratio of the training instance.
                                       learning_rate=learning_rate,
                                       max_depth=max_depth, 

                                       n_jobs=n_jobs, 
                                       base_score=0.5, #The initial prediction score of all instances, global bias.
                                       booster='gbtree', #Specify which booster to use: gbtree, gblinear or dart.
                                       gamma=0, #Minimum loss reduction required to make a further partition on a leaf node of the tree.
                                       max_delta_step=0, #Maximum delta step we allow each tree’s weight estimation to be.
                                       min_child_weight=1, #Minimum sum of instance weight(hessian) needed in a child.
                                       objective='reg:linear', #Specify the learning task and the corresponding learning objective or a custom objective function to be used (see                                        note below).
                                       random_state=random_state,
                                       reg_alpha=0, #L1 regul. on weights
                                       reg_lambda=1,
                                       scale_pos_weight=1,#Balancing of positive and negative weights.
                                       silent=False)


                                #train the model using training set
                                xgb_def.fit(X_train_list[model], y_train_list[model])
                                #predict on the training set using trained model
                                y_hats_train = xgb_def.predict(X_train_list[model])
                                #creates a data frame with concatenated results of all the models (ie. results for each flight
                                #in the period from today up to end of the next next month). Each line is day-flight number (ie flight)
                                #and there should be no duplicated.
                                #The d.f. containjs target column, prediction and columns that identify the flight (like day and airport)
                                y_train_list_results[model][col_train_predict] = y_hats_train
                                data_list[model][col_train_predict] = np.nan
                                data_list[model].loc[y_train_list_results[model].index, col_train_predict] = y_train_list_results[model][col_train_predict]
                                model_results_train_tmp = data_list[model].loc[:, cols_id+[col_target]+[col_train_predict]].dropna(how='any')
                                model_results_train = pd.concat([model_results_train, model_results_train_tmp])
                                #predict on the test set using trained model
                                y_hats = xgb_def.predict(X_test_list[model])
                                #creates a data frame with concatenated results of all the models (ie. results for each flight
                                #in the period from today up to end of the next next month). Each line is day-flight number (ie flight)
                                #and there should be no duplicated.
                                #The d.f. containjs target column, prediction and columns that identify the flight (like day and airport)
                                y_test_list[model][col_predict] = y_hats
                                data_list[model][col_predict] = np.nan
                                data_list[model].loc[y_test_list[model].index, col_predict] = y_test_list[model][col_predict]
                                model_results_tmp = data_list[model].loc[:, cols_id+[col_target]+[col_predict]].dropna(how='any')
                                model_results = pd.concat([model_results, model_results_tmp])
                                #calculate variable importance for the first and the last model
                                if (model_evaluation == 'full'):
                                    if model == 0:
                                        feature_importances = pd.DataFrame(xgb_def.feature_importances_,
                                                                            index = X_test_list[model].columns,
                                                                            columns=['importance']).sort_values('importance', 
                                                                                   ascending=False)
                                        logger.info('Feature importance for the 7 days model \n column' + str(feature_importances))
                                    elif model == 5:
                                        feature_importances = pd.DataFrame(xgb_def.feature_importances_,
                                                                            index = X_test_list[model].columns,
                                                                            columns=['importance']).sort_values('importance', 
                                                                                   ascending=False)
                                        logger.info('Feature importance for 2 months model \n column' + str(feature_importances))
                            #at this point all the models are evaluated for one set of hyperparameters.
                            #the results are gathered inside of one data frame. It this time to evaluate the results for 
                            #given set of hyperparameters
                            #evaluation will be conducted in Pyspark, so we convert the df with results to spark df.
                            model_results_spk = spark.createDataFrame(model_results)
                            model_results_train_spk = spark.createDataFrame(model_results_train)
                            #prints used hyperparameters
                            logger.info('Used parameters are: \n subsamplingRate: ' +\
                            '\n ' + i_param + ': ' + str(i_value) +\
                            '\n ' + j_param + ': ' + str(j_value) +\
                            '\n ' + k_param + ': ' + str(k_value) +\
                            '\n ' + l_param + ': ' + str(l_value) +\
                            '\n ' + m_param + ': ' + str(m_value) )
                            #evaluate the data and prints the metrics on the screen and in the log file
                            logger.info('Evaluation using training set')
                            model_results_train_spk = demand_forecast_model_eval(model_results_train_spk,
                                                                                 col_target,
                                                                                 col_predict=col_train_predict,
                                                                                 model_evaluation='basic',
                                                                                 today_date=first_day_data,
                                                                                 logger=logger)
                            logger.info('Evaluation using test set')
                            model_results_spk = demand_forecast_model_eval(model_results_spk,
                                                                           col_target,
                                                                           col_predict,
                                                                           model_evaluation='basic',
                                                                           today_date=split_value,
                                                                           logger=logger)
        #returns the results for last evaluated model
        if verbose:
            logger.info('XGBoost walk forward forward validation end')
    except Exception:
        logger.exception("Fatal error in xgb_model_eval()")
        raise
    return(model_results, model_results_spk)

