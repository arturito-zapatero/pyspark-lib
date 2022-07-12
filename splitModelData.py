"""
Created on 13.11.2018
@author: aszewczyk
Function that splits data into training and test/pred sets for each of the consecutive models for WFV (walk forward
validation).
Input:
    @allDataList - list with dfs with data for ML model after string indexer, one hot encoder and vector assembler
    (ie. each df has column 'features' which is string of all model features)
    @filterTrainEndList - list with dates denoting training set ends for each for the consecutive models
    (e.g. always yestarday for model in production)
    @filterPredStartList - list with dates denoting test/pred set start for each for the consecutive models
    @filterPredEndList - list with dates denoting test/pred set end for each for the consecutive models
    @training_sample - fraction (if<=1) or number of items (if>1) from training set to use for model training
    @verbose - should print logger messages on the screen and save them to .log file?
	@logger - logger connection
Returns:
    @trainingDataList, @testDataList list with dfs with data for
"""


def splitModelData(
    allDataList: list,
    filterTrainEndList: list,
    filterPredStartList: list,
    filterPredEndList: list,
    training_sample: [int, float],
    verbose: bool,
    logger: bool
):
    try:
        if verbose:
            logger.info('Split into training and test/pred data sets for each of the consecturive models start,'
                        ' function split_model_data_spk()')
        trainingDataList = []
        testDataList = []

        for i in range(len(allDataList)):
            trainingDataList.append(allDataList[i]\
                                    .filter(filterTrainEndList[i])\
                                    .sample(False, training_sample, 23))
            testDataList.append(allDataList[i]\
                                .filter(filterPredStartList[i])\
                                .filter(filterPredEndList[i]))

        if verbose:
            logger.info('Split into training and test/pred data sets for each of the consecturive models end')
    except Exception:
        logger.exception("Fatal error in split_model_data_spk()")
        raise
    return trainingDataList, testDataList


def split_training_data(
    allDataList,
    filterTrainEndList,
    training_sample: [int, float],
    verbose: bool,
    logger: bool
):
    try:
        if verbose:
            logger.info('Split into training data sets for each of the consecturive models start,'
                        ' function split_model_data_spk()')
        trainingDataList = []
        for i in range(len(allDataList)):
            trainingDataList.append(allDataList[i]\
                                    .filter(filterTrainEndList[i])\
                                    .sample(False, training_sample, 23))
        if verbose:
            logger.info('Split into training data sets for each of the consecturive models end')
    except Exception:
        logger.exception("Fatal error in split_training_data()")
        raise
    return trainingDataList


def split_test_data(
    allDataList,
    filterPredStartList,
    filterPredEndList,
    training_sample: [int, float],
    verbose: bool,
    logger: bool
):
    try:
        if verbose:
            logger.info('Split into test/pred data sets for each of the consecturive models start, function split_model_data_spk()')

        testDataList = []
        for i in range(len(allDataList)):
            testDataList.append(allDataList[i]
                                .filter(filterPredStartList[i])\
                                .filter(filterPredEndList[i]))

        if verbose:
            logger.info('Split into test/pred data sets for each of the consecutive models end')
    except Exception:
        logger.exception("Fatal error in split_test_data()")
        raise
    return testDataList
