# -*- coding: utf-8 -*-
"""
One hot encoding using pandas for multiple columns within the data frame. 
Converts categorical variable into dummy/indicator variables, categorical 
columns are afterward removed.
	Input:
		@data - pandas d.f. with data
		@ohe_variables - list with variables' names (strings) to be converted
		@verbose - should print logger messages on the screen and save them to .log file?
		@logger - logger connection
	Returns:
		@data - pandas d.f. with data, with ohe input columns delated and ohe output columns inlcuded
		@ohe_columns_out_names - list with strings with ohe column names after ohe conversion
"""
import pandas as pd
def data_ohe(data,
			 ohe_variables,
			 verbose,
			 logger):
    try:
        if verbose:
            logger.info('One hot encoding starts, function data_ohe(), variables: ')
        try:
            ohe_columns_out_names = list()
            for i in ohe_variables:
                #if data[i].value_counts() is not unique --introduce
                if verbose:
                    logger.info(i)
                if i in data.columns:
                    dummies = pd.get_dummies(data[i], drop_first = True, prefix = i)
                    data = pd.concat([data, dummies], axis=1)
                    data = data.drop([i], axis=1)
                    ohe_columns_out_names = ohe_columns_out_names + list(dummies.columns)

        except Exception, e:
            logger.error('Problem with one hot encoding: ', exc_info=True)
            raise
        if verbose:
            logger.info('One hot encoding end')
    except Exception:
        logger.exception("Fatal error in data_ohe()")
        raise
    return data, ohe_columns_out_names
