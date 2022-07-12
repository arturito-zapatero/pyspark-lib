"""
Created in 2018
@author: aszewczyk
Function that creates a new variable 'sum_'+ranking_cols[i]+'_by_'+ranking_by_col from ranking_cols[i] variables.
New variable is the sum of ranking_cols[i] aggregated by 'ranking_by_col' (in future also by given window of time,
for now over whole 'data')
Input:
    @data - spark df with data including column to be aggregated by and columns to be aggregated
    @ranking_cols - list with strings denoting column name to be aggregated (sum)
    @ranking_time_variable - not used for now, string with time variable name to be aggregated
    @ranking_time_amount - not used for now, int (or date) with time window to be aggregated
    @verbose - should print logger messages on the screen and save them to .log file?
	@logger - logger connection
Returns:
    @data - spark df with with added 'sum_'+ranking_cols[i]+'_by_'+ranking_by_col agggregated columns
TODO:
    - add time variable for aggregations (ranking_time_variable, ranking_time_amount)
"""
import logging

from pyspark.sql.functions import col, udf, lit, sum
from pyspark.sql.types import StringType


def createRankingVariables(
    data,
    ranking_cols: list,
    ranking_by_col: str,
    ranking_time_variable: str,
    ranking_time_amount: str,
    verbose: bool,
    logger: logging.Logger
):
    try:
        if verbose:
            logger.info('Add ranking variables: ranking by: '+ranking_by_col+' ranking cols: '+str(ranking_cols)+
                        ' start, function create_ranking_variables()')
        for i in range(len(ranking_cols)):

            # Create a temporary col
            grouped_df = data.groupBy(ranking_by_col).sum(ranking_cols[i]).\
                                                     withColumnRenamed('sum('+ranking_cols[i]+')',
                                                                       'sum_'+ranking_cols[i]+'_by_'+ranking_by_col)
            data = data.join(grouped_df, ranking_by_col, "inner")
            data = data.drop('sum('+ranking_cols[i]+')')
        if verbose:
            logger.info('Add a new ranking variables: end')
    except Exception:
        logger.exception("Fatal error in create_ranking_variables()")
        raise

    return data
