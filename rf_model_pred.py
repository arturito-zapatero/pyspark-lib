# -*- coding: utf-8 -*-
"""
    Created in Sep.2018
    @author: aszewczyk
    Predict target variable for from tomorrow (split_value) until end of next next month (or end of next next month starting from split_value) using random forest model.
	Several models are created for consecutive prediction periods in the future (ie. one for the next 7 days, next one for days 8-14 and so forth). Each of these models uses       different lag variables (ie. some of lag variables available in 7 days will not be available in 2 months).
    Also inside of each period full model (with all available features) and basic model (with only basic features (ie. lags + flight data), e.g. for new flight with no             historical data) are created.
	
    Input:
		@data_list - list with pandas dfs with data for consecutive models (features, target, id columns)
		@features_list - list with lists with feature variables for each model (ie. each element of the list corresponds to data set in data_list)
        @start_days_list - list with dates denoting prediction start for each of the consecutive models (ie. each element of the list corresponds to data set in data_list)
        @end_days_list - list with dates denoting prediction end for each of the consecutive models (ie. each element of the list corresponds to data set in data_list)
		@col_predict - string with name of column to save the predictions
		@col_train_predict - string with name of column to save the predictions made on training set
		@col_target - string with name of the target variable column
		@cols_id - list with string with names of ID columns, ie. the columns necessary to identify given row of data - flight, this cols are saved together with the results
		@split_value - datetime object denoting date where the prediction starts (ie. tomorrow), training set ends one day earlier 
		@first_day_data - string that denotes date where the training set starts (string of format 'YYYY-mm-dd')
		@show_feat_importance - if True feature importance will be shown on screen and printed to log
        @min_samples - RF hyperparameter: min samples leaf to stop splitting (chosen using model evaluation)
		@max_features - RF hyperparameter: max features on each split (chosen using model evaluation)
		@num_trees - RF hyperparameter: number of trees (chosen using model evaluation)
		@n_jobs - how many processors should model evaluation use 
		@random_state
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
from datetime import datetime
def rf_model_pred(data_list,
                  features_list,
                  features_list_basic_model,
                  start_days_list,
                  end_days_list,
                  col_predict,
                  col_target,
                  cols_id,
                  split_value,
                  split_variable,
                  first_day_data,
                  show_feat_importance,
                  min_samples_leaf,
                  max_features,
                  num_trees,
                  n_jobs,
                  random_state,
                  spark,
                  verbose,
                  logger
                   ):
    try:
        if verbose:
            logger.info('Random Forest predictions for each flight start, function rf_model_pred()')
            logger.info('First day of prediction: ' + split_value)
            logger.info('Prediction until the last day of the next next month')
            logger.info('Full features list for the : ' + str(features_list[0])) #list with string denoting column names of the model for next 7 days
            logger.info('Target variable: ' + str(col_target))
            logger.info('Comments: none')

        #create data frames to save the results
        model_results = pd.DataFrame()
        #model_results_train = pd.DataFrame()
        #here a loop for each of the data sets/models
        for model in range(len(data_list)):
            start = datetime.now()
            if verbose:
                logger.info('Start model for days from: ' + str(start_days_list[model]) + ' to ' + str(end_days_list[model]))
            #start = time.time()
            #split data for training and prediction set, first all available features for full model
            X_train_i = data_list[model]\
                                  .loc[(data_list[model][split_variable] < split_value), features_list[model]]\
                                  .dropna(how='any').astype(float)
            #now basic model, for rows/lines with Nulls (e.g. for a new flight), use only basic features (ie. lags + flight data)
            X_train_basic_i = data_list[model]\
                                  .loc[(data_list[model][split_variable] < split_value), features_list_basic_model[model]]\
                                  .dropna(how='any').astype(float)
            y_train_i = data_list[model]\
                                  .loc[(data_list[model][split_variable] < split_value), col_target]\
                                  .dropna(how='any').astype(float)
            #full data set for prediction
            X_pred_all_i = data_list[model]\
                                  .loc[(data_list[model][split_variable] >= start_days_list[model]) & (data_list[model][split_variable] < end_days_list[model]),:]                   
            #data set for prediction with removed nulls - for model with full features list
            X_pred_i = data_list[model]\
                                  .loc[(data_list[model][split_variable] >= start_days_list[model]) & (data_list[model][split_variable] < end_days_list[model]),
                                       features_list[model]]\
                                  .dropna(how='any').astype(float)
            #data set for prediction with only rows containing nulls, for model with basic features (ie. lags + flight data)
            X_pred_basic_i = X_pred_all_i[~X_pred_all_i.index.isin(X_pred_i.index)].loc[:,features_list_basic_model[model]].astype(float)
            #leave as a placeholders to collect predictions (only id columns)
            y_pred_i = data_list[model]\
                                      .loc[(data_list[model][split_variable] >= start_days_list[model]) & (data_list[model][split_variable] < end_days_list[model]),
                                           cols_id]\
                                      .dropna(how='any')
            y_pred_basic_i = X_pred_all_i[~X_pred_all_i.index.isin(X_pred_i.index)].loc[:,cols_id]
            #make sure the both data sets have the same length
            X_train_i = X_train_i[X_train_i.index.isin(y_train_i.index)]
            y_train_i = y_train_i[y_train_i.index.isin(X_train_i.index)]
            #the same for basic model
            X_train_basic_i = X_train_basic_i[X_train_basic_i.index.isin(y_train_i.index)]
            y_train_basic_i = y_train_i[y_train_i.index.isin(X_train_basic_i.index)]
            X_pred_i = X_pred_i[X_pred_i.index.isin(y_pred_i.index)]
            y_pred_i = y_pred_i[y_pred_i.index.isin(X_pred_i.index)]
            #define the model objects for full and basic models
            rf_model_def = RandomForestRegressor(n_estimators=num_trees,
                                                  max_features=max_features, 
                                                  min_samples_leaf=min_samples_leaf,
                                                  oob_score=False,
                                                  bootstrap=True,
                                                  random_state = random_state,
                                                  n_jobs=n_jobs)
            rf_basic_model_def = RandomForestRegressor(n_estimators=num_trees,
                                                  max_features=max_features, 
                                                  min_samples_leaf=min_samples_leaf,
                                                  oob_score=False,
                                                  bootstrap=True,
                                                  random_state = random_state,
                                                  n_jobs=n_jobs)
            #create placeholder column for predicted values
            data_list[model][col_predict] = np.nan
            if not (X_train_i.empty and y_train_i.empty and X_pred_i.empty):
                #train the model
                rf_model_def.fit(X_train_i, y_train_i)
                #predict
                y_hats = rf_model_def.predict(X_pred_i)
                #creates a data frame with concatenated results of all the models (ie. results for each flight
                #in the period from today up to end of the next next month). Each line is day-flight number (ie flight)
                #and there should be no duplicates
                #The d.f. contains target column, prediction and id columns that identify the flight (like day and airport)
                y_pred_i[col_predict] = y_hats
                data_list[model].loc[y_pred_i.index, col_predict] = y_pred_i[col_predict]
                #calculate variable importance for the first and the last from full model
                if (show_feat_importance == 'full' and verbose):
                    if model == 0:
                        feature_importances_first = pd.DataFrame(rf_model_def.feature_importances_,
                                                            index = X_pred_i.columns,
                                                            columns=['importance']).sort_values('importance', 
                                                                   ascending=False)
                        logger.info('Feature importance for the 7 days all features model \n column' + str(feature_importances_first))
                    elif model == 5:
                        feature_importances_last = pd.DataFrame(rf_model_def.feature_importances_,
                                                            index = X_pred_i.columns,
                                                            columns=['importance']).sort_values('importance', 
                                                                   ascending=False)
                        logger.info('Feature importance for 2 months all features model \n column' + str(feature_importances_last))
                else:
                    feature_importances_first = None
                    feature_importances_last = None
            #analogously for basic model
            #first delete Nulls if still exist (should not be the case)
            if (not X_train_basic_i.empty and not y_train_basic_i.empty and not X_pred_basic_i.empty):
                X_train_basic_i = X_train_basic_i.dropna(how='any')
                y_train_basic_i = y_train_basic_i.dropna(how='any')
                X_pred_basic_i = X_pred_basic_i.dropna(how='any')
                y_pred_basic_i = y_pred_basic_i.dropna(how='any')
                #make sure the X and y data sets have the same length
                X_train_basic_i = X_train_basic_i[X_train_basic_i.index.isin(y_train_basic_i.index)]
                y_train_basic_i = y_train_basic_i[y_train_basic_i.index.isin(X_train_basic_i.index)]
                X_pred_basic_i = X_pred_basic_i[X_pred_basic_i.index.isin(y_pred_basic_i.index)]
                y_pred_basic_i = y_pred_basic_i[y_pred_basic_i.index.isin(X_pred_basic_i.index)]
            if (not X_train_basic_i.empty and not y_train_basic_i.empty and not X_pred_basic_i.empty):
                #fit model and predict
                rf_basic_model_def.fit(X_train_basic_i, y_train_basic_i)
                y_basic_hats = rf_basic_model_def.predict(X_pred_basic_i)
                y_pred_basic_i[col_predict] = y_basic_hats 
                data_list[model].loc[y_pred_basic_i.index, col_predict] = y_pred_basic_i[col_predict]
            #concatenate results for model nr 'model':
            if not data_list[model].empty:
                model_results_tmp = data_list[model].loc[:, cols_id+[col_predict]].dropna(how='any')
                model_results = pd.concat([model_results, model_results_tmp])
            end = datetime.now()
            if verbose:
                logger.info('Model ' + str(model) + ' execution time: ' + str(end-start))
        #create also spark data frame with results
        model_results_spk = spark.createDataFrame(model_results)
        if verbose:
            logger.info('Random Forest predictions for each flight end')
        
    except Exception:
        logger.exception("Fatal error in rf_model_pred()")
        raise
    return(model_results, model_results_spk, feature_importances_first, feature_importances_last)