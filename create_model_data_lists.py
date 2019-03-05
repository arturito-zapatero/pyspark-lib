"""
Created on 13.11.2018
@author: aszewczyk
Function that splits data into training and test/pred sets for each of the consecutive models, for 'full' and 'basic' models.
Actually it only splits into basic and full models now!!
It depends on target variable (each target variable has deifferent features)
Basic models are created for rows/flights with missing data (e.g. new flights with no history), these models are trained
using only 'basic' features.
Input:
    @data - df with data after cleaning (with features, target vars and other vars)
    @features_list - list with lists of features (as strings) for each of the consecutive 'full' models, w/o OHE vars
    @features_list_basic_model - as above but for 'basic' models
    @ohe_variables_in - list with strings denoting ohe variables
    #@col_target - string with target variable
    @cols_id - list with strings denoting id variable (not features, variables used to describe each row/flight),
    this variables are saved together with the results
    @filterTrainEndList - list with dates denoting training set ends for each for the consecutive models
    (e.g. always yestarday for model in production)
    @filterPredStartList - list with dates denoting test/pred set start for each for the consecutive models
    @filterPredEndList - list with dates denoting test/pred set end for each for the consecutive models
    @use_clustered_data_sets - if train model on cluster level, or on full data
    @verbose - should print logger messages on the screen and save them to .log file?
	@logger - logger connection
Returns:
    @data_list - list with spark dfs with data for each of the consecutive 'full' models
    @data_basic_list - as above but for 'basic' models
"""
import pandas as pd
from pyspark.sql.functions import col
def create_model_data_lists(data,
                            features_list,
                            features_list_basic_model,
                            ohe_variables_in,
                            cols_id,
                            filterTrainEndList,
                            filterPredStartList,
                            filterPredEndList,
                            use_clustered_data_sets,
							spark,
                            verbose,
                            logger):
    try:
        if verbose:
            logger.info('Function create_model_data_lists() start')
        data_list = []
        data_basic_list = []
        for i in range(len(features_list)):
            if verbose:
                logger.info('Creating data set for ' + str(i) + 'th consecutive model')
            if use_clustered_data_sets:
                #create data sets for each of the models, includes cluster variables
                data_list\
                .append(data\
                        .select(features_list[i] + ohe_variables_in + ['pax_seat', 'revenue_tickets', 'ancis'] + cols_id + ['cluster', 'cluster_afp']))
            else:
                #create data sets for each of the models, w/o cluster variables
                data_list\
                .append(data\
                        .select(features_list[i] + ohe_variables_in + ['pax_seat', 'revenue_tickets', 'ancis'] + cols_id))
            #as above, but for flights/rows with NANs for prediction set (e.g. new routes, no historical data),
            #create new data sets for this rows/flights using only basic features for i-th consecturive model
            data_train_full = data_list[i]\
                              .filter(filterTrainEndList[i])
            data_pred_full = data_list[i]\
                             .filter(filterPredStartList[i])\
                             .filter(filterPredEndList[i])
            data_pred_no_nulls = data_list[i]\
                                 .filter(filterPredStartList[i])\
                                 .filter(filterPredEndList[i])\
                                 .dropna(how='any')
            #only rows with Nulls, first line not works (Spark bug), we need workaround:
            #data_pred_nulls = data_pred_full.subtract(data_pred_no_nulls)#
            data_pred_nulls = data_pred_full.join(data_pred_no_nulls.selectExpr('"no" as row_with_nulls', 'dt_flight_date_local', 'cd_num_flight', 'cd_airport_pair'),
                        ['dt_flight_date_local', 'cd_num_flight', 'cd_airport_pair'], "leftouter").where(col('row_with_nulls').isNull()).drop('row_with_nulls')
            #join with training data for i-th basic model (all these selects are just because we need the same column order, which was lost during join)
            data_basic_full = data_pred_nulls.union(data_train_full.selectExpr('dt_flight_date_local as dt_flight_date_local1', 'cd_num_flight as cd_num_flight1', 'cd_airport_pair as cd_airport_pair1', '*')\
                         .drop(*['dt_flight_date_local', 'cd_num_flight', 'cd_airport_pair'])\
                         .selectExpr('dt_flight_date_local1 as dt_flight_date_local', 'cd_num_flight1 as cd_num_flight', 'cd_airport_pair1 as cd_airport_pair', '*')\
                         .drop(*['dt_flight_date_local1', 'cd_num_flight1', 'cd_airport_pair1']))
            #data_basic_full = data_pred_nulls\
            #                  .union(data_train_full)        
            #append to list with data for i-th consecutive basic model
            data_basic_list.append(data_basic_full.select(features_list_basic_model[i] + ohe_variables_in + ['pax_seat', 'revenue_tickets', 'ancis'] + cols_id))
            #drop NAs
            data_list[i] = data_list[i].dropna(how='any', subset=features_list[i] + ohe_variables_in)
            data_basic_list[i].dropna(how='any', subset=features_list_basic_model[i] + ohe_variables_in)
        if verbose:
            logger.info('Data sets for all models created')
    except Exception:
        logger.exception("Fatal error in create_model_data_lists()")
        raise
    return(data_list, data_basic_list)

"""
Created on 13.11.2018
@author: aszewczyk
Function that splits data into training sets for each of the consecutive models, for 'full' and 'basic' models (6+6 models).
Each full or basic model is for different prediction time window (ie. models for next 7 days will have more variables
than model for days 60-90) 
It depends also on target variable (each target variable has different features)
Basic models are created for rows/flights with missing data (e.g. new flights with no history), these models are trained
using only 'basic' features.
Input:
    @data - df with data after cleaning (with features, target vars and other vars)
    @features_list - list with lists of features (as strings) for each of the consecutive 'full' models, w/o OHE vars
    @features_list_basic_model - as above but for 'basic' models
    @ohe_variables_in - list with strings denoting ohe variables
    @col_target - string with target variable
    @cols_id - list with strings denoting id variable (not features, variables used to describe each row/flight),
    this variables are saved together with the results
    @filterTrainEndList - list with dates denoting training set ends for each for the consecutive models
    (e.g. always yestarday for model in production)
    @use_clustered_data_sets - if train model on cluster level, or on full data
    @spark - spark session
    @verbose - should print logger messages on the screen and save them to .log file?
	@logger - logger connection
Returns:
    @data_list - list with spark dfs with data for each of the consecutive 'full' models
    @data_basic_list - as above but for 'basic' models
"""
def create_training_data_lists(data,
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
                               logger):
    try:
        if verbose:
            logger.info('Function create_training_data_lists() start')
        trainingDataList = []
        trainingDataBasicList = []
        #for each prediction time window
        for i in range(len(features_list)):
            if verbose:
                logger.info('Creating training data set for ' + str(i) + 'th consecutive model')
            #1.select only features, target variable and if columns
            if use_clustered_data_sets:
                #create data sets for each of the models, includes cluster variables
                trainingDataList\
                .append(data\
                        .select(features_list[i] + ohe_variables_in + [col_target] + cols_id + ['cluster', 'cluster_afp']))
            else:
                #create data sets for each of the models, w/o cluster variables
                trainingDataList\
                .append(data\
                        .select(features_list[i] + ohe_variables_in + [col_target] + cols_id))
            #2.split into training set by dates, sample
            trainingDataList[i] = trainingDataList[i]\
                                  .filter(filterTrainEndList[i])\
                                  .sample(False, training_sample, 23)
            #3.create basic data lists
            #creates basic training list for flights/rows with NANs in prediction set (e.g. new routes, no historical data),
            #create new training data sets using only basic features for i-th consectutive model
            trainingDataBasicList.append(trainingDataList[i].\
                                         select(features_list_basic_model[i] + ohe_variables_in + [col_target] + cols_id))
            #4.drop NAs
            trainingDataList[i] = trainingDataList[i].dropna(how='any', subset=features_list[i] + ohe_variables_in)
            #drop NAs for basic model
            trainingDataBasicList[i].dropna(how='any', subset=features_list_basic_model[i] + ohe_variables_in)
        if verbose:
            logger.info('Split into training data sets for each of the consecturive models end')
        if verbose:
            logger.info('Data sets for all models created')
    except Exception:
        logger.exception("Fatal error in create_training_data_lists()")
        raise
    return(trainingDataList, trainingDataBasicList)
"""
Created on 13.11.2018
@author: aszewczyk
Function that splits data into test sets for each of the consecutive models, for 'full' and 'basic' models (6+6 models).
Each full or basic model is for different prediction time window (ie. models for next 7 days will have more variables
than model for days 60-90, also different time windows) 
It depends also on target variable (each target variable has different features)
Basic models are created for rows/flights with missing data (e.g. new flights with no history), these models are trained
using only 'basic' features.
Input:
    @data - df with data after cleaning (with features, target vars and other vars)
    @features_list - list with lists of features (as strings) for each of the consecutive 'full' models, w/o OHE vars
    @features_list_basic_model - as above but for 'basic' models
    @ohe_variables_in - list with strings denoting ohe variables
    @col_target - string with target variable
    @cols_id - list with strings denoting id variable (not features, variables used to describe each row/flight),
    this variables are saved together with the results
    @filterPredStartList - list with dates denoting test/pred set start for each for the consecutive models
    @filterPredEndList - list with dates denoting test/pred set end for each for the consecutive models
    @use_clustered_data_sets - if train model on cluster level, or on full data
    @spark - spark session
    @verbose - should print logger messages on the screen and save them to .log file?
	@logger - logger connection
Returns:
    @data_list - list with spark dfs with data for each of the consecutive 'full' models
    @data_basic_list - as above but for 'basic' models
"""
def create_test_data_lists(data,
                            features_list,
                            features_list_basic_model,
                            ohe_variables_in,
                            col_target,
                            cols_id,
                            filterPredStartList,
                            filterPredEndList,
                            use_clustered_data_sets,
							spark,
                            verbose,
                            logger):
    try:
        if verbose:
            logger.info('Function create_model_data_lists() start')
        testDataList = []
        testDataBasicList = []
        #for each prediction time window
        for i in range(len(features_list)):
            if verbose:
                logger.info('Creating data set for ' + str(i) + 'th consecutive model')
            #1.select only features, target variable and if columns
            #2.divide by dates into test sets
            if use_clustered_data_sets:
                #create data sets for each of the models, includes cluster variables
                testDataList\
                .append(data\
                        .select(features_list[i] + ohe_variables_in + [col_target] + cols_id + ['cluster', 'cluster_afp'])\
                .filter(filterPredStartList[i])\
                .filter(filterPredEndList[i]))
            else:
                #create data sets for each of the models, w/o cluster variables
                testDataList\
                .append(data\
                        .select(features_list[i] + ohe_variables_in + [col_target] + cols_id)\
                .filter(filterPredStartList[i])\
                .filter(filterPredEndList[i]))
            #3.create basic data lists
            #creates basic test list for flights/rows with NANs for prediction set (e.g. new routes, no historical data),
            #create new data sets for this rows/flights using only basic features for i-th consectutive model
            #a)create full data
            data_pred_full = testDataList[i]
            #b)create data without NAs
            data_pred_no_nulls = testDataList[i]\
                                 .dropna(how='any')
            #subtruct b) from a) to obtain only rows with NAs
            data_pred_nulls = data_pred_full.join(data_pred_no_nulls.selectExpr('"no" as row_with_nulls', 'dt_flight_date_local', 'cd_num_flight', 'cd_airport_pair'),
                        ['dt_flight_date_local', 'cd_num_flight', 'cd_airport_pair'], "leftouter").where(col('row_with_nulls').isNull()).drop('row_with_nulls')
               
            #d) selects only basic variables
            testDataBasicList.append(data_pred_nulls.select(features_list_basic_model[i] + ohe_variables_in + [col_target] + cols_id))
            #drop NAs
            testDataList[i] = testDataList[i].dropna(how='any', subset=features_list[i] + ohe_variables_in)
            testDataBasicList[i].dropna(how='any', subset=features_list_basic_model[i] + ohe_variables_in)
            

        if verbose:
            logger.info('Split into test/pred data sets for each of the consectutive models end')
        if verbose:
            logger.info('Data sets for all models created')
    except Exception:
        logger.exception("Fatal error in create_model_data_lists()")
        raise
    return(testDataList, testDataBasicList)

