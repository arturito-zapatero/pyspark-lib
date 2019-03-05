# -*- coding: utf-8 -*-
"""
Created in 2018
@author: aszewczyk
Function that splits (for model evaluation) data in training and test set for each 
of datasets in data_list (ie. each consecutive model), It creates lists with training
 (_train_) and test (_test_) sets with feature columns (X_) and target column (y_)
 which are ready to be used as input for each of the models.
Input:
    @training_set_range - 'full': full set of data used to train a model (all data until split_value)
                          'last_year_period' - only last year period data  +/- 30 days used to train a model 
                          (ie. training set period = test period - 1 year  +/- 30 days)
    @data_list - list with pandas dfs with data for consecutive models (features, target, id columns)
    @split_value - datetime object denoting date where the test set starts (ie. training sets ends one day before)
    @split_variable - string denoting the name of column used for split (normally date) 
    @start_days_list - list with dates denoting test set start for each of the consecutive models
    (ie. each element of the list corresponds to data set in data_list)
    @end_days_list - list with dates denoting test set end for each of the consecutive models
    (ie. each element of the list corresponds to data set in data_list)
    @features_list - list with string denoting column names of the first model (for the next 7 days) 
    @col_target - string with name of the target column
	@verbose - should print logger messages on the screen and save them to .log file?
	@logger - logger connection
Returns:
    @X_train_list - list with pandas dfs with training feature columns
    @y_train_list - list with pandas dfs with training target column
    @X_test_list - list with pandas dfs with test feature columns
    @y_test_list - list with pandas dfs with test target column, also here the test set evaluation will be saved
    @y_train_list_results - copy of list with pandas dfs with training target column to save training set evaluation
"""
import pandas as pd
from datetime import timedelta
from datetime import datetime
def split_data(training_set_range,
               data_list,
               split_value,
               split_variable,
               start_days_list,
               end_days_list,
               features_list,
               col_target,
               verbose,
               logger):
    try:
        if verbose:
            logger.info('Split data into training and test sets start, function split_data()')
        #create empty lists to append to
        X_train_list = []
        y_train_list = []
        X_test_list  = []
        y_test_list  = []
        y_train_list_results = []
        #full set of data used to train a model (all data until split_value)
        if training_set_range=='full':
            #split into test and train set for each of the data sets in data_list and append to prepared lists:
            for i in range(len(data_list)):
                X_train_list.append(pd.DataFrame(data_list[i]\
                                      .loc[(data_list[i][split_variable] < split_value), features_list[i]]\
                                      .astype(float)))
                y_train_list.append(pd.DataFrame(data_list[i]\
                                      .loc[(data_list[i][split_variable] < split_value), col_target]\
                                      .astype(float)))
                X_test_list.append(pd.DataFrame(data_list[i]\
                                      .loc[(data_list[i][split_variable] >= start_days_list[i]) & (data_list[i][split_variable] <= end_days_list[i]),
                                           features_list[i]]\
                                      .astype(float)))
                y_test_list.append(pd.DataFrame(data_list[i]\
                                      .loc[(data_list[i][split_variable] >= start_days_list[i]) & (data_list[i][split_variable] <= end_days_list[i]),
                                           col_target]\
                                      .astype(float)))
                #copy training table to add results
                y_train_list_results.append(pd.DataFrame(data_list[i]\
                                      .loc[(data_list[i][split_variable] < split_value), col_target]\
                                      .astype(float)))
        #only last year period data  +/- 30 days used to train a model 
        if training_set_range=='last_year_period':
            #split into test and train set for each of the data sets in data_list and append to prepared lists:
            for i in range(len(data_list)):
                X_train_list.append(pd.DataFrame(data_list[i]\
                                      .loc[(data_list[i][split_variable] <= (datetime.strptime(end_days_list[i],   "%Y-%m-%d") - (timedelta(days=365-30)))) &
                                           (data_list[i][split_variable] >= (datetime.strptime(start_days_list[i], "%Y-%m-%d") - (timedelta(days=365+30)))),
                                           features_list[i]]\
                                      .astype(float)))
                y_train_list.append(pd.DataFrame(data_list[i]\
                                      .loc[(data_list[i][split_variable] <= (datetime.strptime(end_days_list[i],   "%Y-%m-%d") - (timedelta(days=365-30)))) &
                                           (data_list[i][split_variable] >= (datetime.strptime(start_days_list[i], "%Y-%m-%d") - (timedelta(days=365+30)))),
                                           col_target]\
                                      .astype(float)))
                X_test_list.append(pd.DataFrame(data_list[i]\
                                      .loc[(data_list[i][split_variable] >= start_days_list[i]) & (data_list[i][split_variable] <= end_days_list[i]),
                                           features_list[i]]\
                                      .astype(float)))
                y_test_list.append(pd.DataFrame(data_list[i]\
                                      .loc[(data_list[i][split_variable] >= start_days_list[i]) & (data_list[i][split_variable] <= end_days_list[i]),
                                           col_target]\
                                      .astype(float)))
                #copy training table to add results
                y_train_list_results.append(pd.DataFrame(data_list[i]\
                                      .loc[(data_list[i][split_variable] <= (datetime.strptime(end_days_list[i],   "%Y-%m-%d") - (timedelta(days=365-30)))) &
                                           (data_list[i][split_variable] >= (datetime.strptime(start_days_list[i], "%Y-%m-%d") - (timedelta(days=365+30)))),
                                           col_target]\
                                      .astype(float)))
        if verbose:
            logger.info('Split data into training and test sets end')
    except Exception:
        logger.exception("Fatal error in split_data()")
        raise
    return(X_train_list,
			y_train_list,
			X_test_list,
			y_test_list,
			y_train_list_results)
