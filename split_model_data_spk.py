"""
Created on 13.11.2018
@author: aszewczyk
NOT USED FOR DEMAND FORECAST ANYMORE, parts of function create_model_data_list
Function that splits data into training and test/pred sets for each of the consecutive models, for 'full' and 'basic' models
Input:
    @allDataList - list with dfs with data for 'full' models after string indexer, one hot encoder and vector assembler 
    (ie. each df has column 'features' which is string of all model features)
    @allDataBasicList - as above but for 'basic' model
    @filterTrainEndList - list with dates denoting training set ends for each for the consecutive models
    (e.g. always yestarday for model in production)
    @filterPredStartList - list with dates denoting test/pred set start for each for the consecutive models
    @filterPredEndList - list with dates denoting test/pred set end for each for the consecutive models
    @training_sample - fraction (if<=1) or number of items (if>1) from training set to use for model training
    @verbose - should print logger messages on the screen and save them to .log file?
	@logger - logger connection
Returns:
    feature_importances - df with features and feat. importances, ordered by feat. importances
"""
def split_model_data_spk(allDataList,
                         allDataBasicList,
                         filterTrainEndList,
                         filterPredStartList,
                         filterPredEndList,
                         training_sample,
                         verbose,
                         logger):
    try:
        if verbose:
            logger.info('Split into training and test/pred data sets for each of the consecturive models start, function split_model_data_spk()')
        trainingDataList = []
        testDataList = []
        trainingDataBasicList = []
        testDataBasicList = []
        for i in range(len(allDataList)):
            #'full' model
            trainingDataList\
            .append(allDataList[i]\
                    .filter(filterTrainEndList[i])\
                    .sample(False, training_sample, 23))
            testDataList\
            .append(allDataList[i]\
                    .filter(filterPredStartList[i])\
                    .filter(filterPredEndList[i]))
            #'basic model'
            trainingDataBasicList\
            .append(allDataBasicList[i]\
                    .filter(filterTrainEndList[i])\
                    .sample(False, training_sample, 23))
            testDataBasicList\
            .append(allDataBasicList[i]\
                    .filter(filterPredStartList[i])\
                    .filter(filterPredEndList[i]))
        if verbose:
            logger.info('Split into training and test/pred data sets for each of the consecturive models end')
    except Exception:
        logger.exception("Fatal error in split_model_data_spk()")
        raise
    return(trainingDataList, testDataList, trainingDataBasicList, testDataBasicList)
def split_training_data(allDataList,
                        allDataBasicList,
                        filterTrainEndList,
                        #filterPredStartList,
                        #filterPredEndList,
                        training_sample,
                        verbose,
                        logger):
    try:
        if verbose:
            logger.info('Split into training data sets for each of the consecturive models start, function split_model_data_spk()')
        trainingDataList = []
        #testDataList = []
        trainingDataBasicList = []
        #testDataBasicList = []
        for i in range(len(allDataList)):
            #'full' model
            trainingDataList\
            .append(allDataList[i]\
                    .filter(filterTrainEndList[i])\
                    .sample(False, training_sample, 23))
            #'basic model'
            trainingDataBasicList\
            .append(allDataBasicList[i]\
                    .filter(filterTrainEndList[i])\
                    .sample(False, training_sample, 23))
        if verbose:
            logger.info('Split into training data sets for each of the consecturive models end')
    except Exception:
        logger.exception("Fatal error in split_training_data()")
        raise
    return(trainingDataList, trainingDataBasicList)
def split_test_data(allDataList,
                    allDataBasicList,
                    #filterTrainEndList,
                    filterPredStartList,
                    filterPredEndList,
                    training_sample,
                    verbose,
                    logger):
    try:
        if verbose:
            logger.info('Split into test/pred data sets for each of the consecturive models start, function split_model_data_spk()')
        #trainingDataList = []
        testDataList = []
        #trainingDataBasicList = []
        testDataBasicList = []
        for i in range(len(allDataList)):
            #'full' model
            testDataList\
            .append(allDataList[i]\
                    .filter(filterPredStartList[i])\
                    .filter(filterPredEndList[i]))
            #'basic model'
            testDataBasicList\
            .append(allDataBasicList[i]\
                    .filter(filterPredStartList[i])\
                    .filter(filterPredEndList[i]))
        if verbose:
            logger.info('Split into test/pred data sets for each of the consecturive models end')
    except Exception:
        logger.exception("Fatal error in split_test_data()")
        raise
    return(testDataList, testDataBasicList)