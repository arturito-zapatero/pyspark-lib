# -*- coding: utf-8 -*-
"""
Created on Thu May 24 10:36:02 2018
@author: aszewczyk
Defines the features and target columns for ML models, also categorical features for OHE, cyclical features, ID columns
(ie. columns not used in ML model, but necessary for identification purposes) and other columns if necessary
(ie. date column)

Input:
    @model_complex - model complexity, more complex model will use more features
    @use_clustered_data_sets - if model is trained and tested/predicted using clusters or whole data set
    @col_target - string with target column
    @verbose - should print logger messages on the screen and save them to .log file?
		@logger - logger connection
Returns:
    @cols_features - list of strings with feature columns for ML model (with complexity=model_complex),
    excluding the ohe columns and cyclical columns
    @cols_cyclical - list of strings with cyclical columns
    @ohe_variables_in - list of strings with categorical columns' names that are transformed in OHE process
    @col_target - string with target columns
    @cols_id - list with string with ID columns
"""

def defineFeatures(
    model_complex: str = 'first',
    use_clustered_data_sets: bool = False,
    col_target: str = 'target_column',
    verbose: bool = False,
    logger: bool = False
) -> [list, list, list, str, list]:
    try:
        if verbose:
            logger.info('Definition of features and target columns start, function define_features_and_target_columns()')

        # Define one hot encoding variables
        if model_complex == 'first':
            cols_cyclical = []
            if use_clustered_data_sets:
                cols_ohe_in = []
            else:
                cols_ohe_in = []
            cols_features = []

        col_target = ''
        cols_id = []

        if verbose:
            logger.info('Definition of features and target columns end')
    except Exception:
        logger.exception("Fatal error in define_features_and_target_columns()")
        raise
    return cols_cyclical, cols_ohe_in, cols_features, col_target, cols_id
