"""
Function that adds error (SE, APE and AE) columns to spark df with model results
Input:
    @transformedFull - spark df with model results
    @col_target - string with target column name
    @col_predict - string with prediction column name
    @verbose - should print logger messages on the screen and save them to .log file?
	@logger - logger connection
Returns:
    @transformedFull - spark df with model results with added error columns
"""
import logging
from pyspark.sql.functions import abs, pow


def addErrorCols(
    transformedFull,
    col_target: str,
    col_predict: str,
    verbose: bool,
    logger: logging.Logger
):

    try:
        if verbose:
            logger.info('Add error columns to spark df start, function add_error_cols()')
        transformedFull = transformedFull\
                         .select('*', abs((transformedFull[col_target] - transformedFull[col_predict])
                                         /transformedFull[col_target]*100)\
                         .alias(col_target+'_APE'))
        transformedFull = transformedFull\
                         .select('*', abs((transformedFull[col_target] - transformedFull[col_predict]))\
                         .alias(col_target+'_AE'))
        transformedFull = transformedFull\
                         .select('*', pow(transformedFull[col_target] - transformedFull[col_predict], 2)\
                         .alias(col_target+'_SE'))

        if verbose:
            logger.info('Add error columns to spark df end')
    except Exception:
        logger.exception("Fatal error in add_error_cols()")
        raise

    return transformedFull
