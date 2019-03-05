# -*- coding: utf-8 -*-
"""
Created in 2018
@author: aszewczyk
Function that creates a new variable 'cd_airport_pair_reduced' from 'cd_airport_pair'.
For rows with largest (counting revenue) count_threshold segments it keep the 'cd_airport_pair'
value, for segment with lower values of revenue 'minor_segment' value is assigned.
Input:
    @ds_flight_master - spark df with flight master data
    @count_threshold - how many largest segment should keep their values of 'cd_airport_pair',
    for segment with lower values of revenue 'minor_segment' value is assigned
    @verbose - should print logger messages on the screen and save them to .log file?
		@logger - logger connection
Returns:
    @ds_flight_master - spark df with flight master data, with added 'cd_airport_pair_reduced' column
"""
from pyspark.sql.functions import col, udf, lit, sum
from pyspark.sql.types import StringType
#from convertMinority import convertMinority
def create_reduced_airport_pair_variable(ds_flight_master,
										 count_threshold,
                                         verbose,
                                         logger):
    
    """
    Function that for rows where value of sum_revenue column is smaller than threshold_revenue changes  
    originalCol value to 'minor_segment', otherwise changes nothing
    """
    def convertMinority(threshold_revenue,
                        originalCol,
                        sum_revenue):
        if sum_revenue > threshold_revenue:
            return originalCol
        else:
            return 'minor_segment'
    try:
        if verbose:
            logger.info('Add a new variable: cd_airport_pair_reduced start, function create_reduced_airport_pair_variable()')
        #create a temporary col "sum_revenue_tickets" for each "cd_airport_pair"
        cd_airport_pair_count = ds_flight_master.groupBy("cd_airport_pair").sum('revenue_tickets').\
                                                 withColumnRenamed('sum(revenue_tickets)', 'sum_revenue_tickets').\
                                                 sort(col('sum_revenue_tickets').desc())
        #find revenue value (on the segment level) for the count_threshold'th segment
        threshold_revenue = cd_airport_pair_count.toPandas().loc[count_threshold-1, 'sum_revenue_tickets']
        ds_flight_master = ds_flight_master.join(cd_airport_pair_count, "cd_airport_pair", "inner")
        createNewColFromTwo = udf(convertMinority, StringType())
        #replace name value of 'cd_airport_pair' with string 'minor_segment' for segments with smallests revenues
        ds_flight_master = ds_flight_master.withColumn('cd_airport_pair_reduced',
                                                       createNewColFromTwo(lit(threshold_revenue), ds_flight_master['cd_airport_pair'],
                                                                           ds_flight_master['sum_revenue_tickets']))
        ds_flight_master = ds_flight_master.drop('sum_revenue_tickets')
        if verbose:
            logger.info('Add a new variable: cd_airport_pair_reduced start, end')
    except Exception:
        logger.exception("Fatal error in create_reduced_airport_pair_variable()")
        raise
    return(ds_flight_master)
