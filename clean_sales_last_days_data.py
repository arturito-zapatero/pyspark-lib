# -*- coding: utf-8 -*-
"""
Created in 2018
@author: aszewczyk
Function that cleans typical sales last year data as follows:
    - converts absolute values (pax, revenue etc.) to relative values (l.f. and yields)
    - rename last year prices columns
    - drop unnecessary columns
Input:
    @ds_sales_last_days_seasonal - sales last day data to be cleaned data before cleaning, 
    aggregated on seasonal level (DOW, TOD, airport pair)
    @ds_sales_last_days_airport_pair - sales last day data to be cleaned data before cleaning,
    aggregated in airport pair level
    @named_variables_lists - lists with variables used for demand forecast
 		@verbose - should print logger messages on the screen and save them to .log file?
		@logger - logger connection
Returns:
    @ds_sales_last_days_seasonal, ds_sales_last_days_airport_pair - sales last days data after cleaning
"""
def clean_sales_last_days_data(ds_sales_last_days_seasonal,
                               ds_sales_last_days_airport_pair,
                               named_variables_lists,
                               verbose,
                               logger):
    try:
        if verbose:
            logger.info('clean_sales_last_days_data() start')
        #load relevant variables used in this function
        last_days_airport_pair_abs_cols = named_variables_lists['last_days_airport_pair_abs_cols']
        last_days_seasonal_abs_cols = named_variables_lists['last_days_seasonal_abs_cols']
        last_days_airport_pair_rel_cols = named_variables_lists['last_days_airport_pair_rel_cols']
        last_days_seasonal_rel_cols = named_variables_lists['last_days_seasonal_rel_cols']
        #convert absolute values (pax, revenue etc.) to relative values (l.f. and yields) on the airport pair level
        for i in range(len(last_days_airport_pair_abs_cols)):       
            ds_sales_last_days_airport_pair = ds_sales_last_days_airport_pair.\
            select('*', (ds_sales_last_days_airport_pair[last_days_airport_pair_abs_cols[i]]/ds_sales_last_days_airport_pair['capacity_airport_pair'])\
             .alias(last_days_airport_pair_rel_cols[i]))
        #convert absolute values (pax, revenue etc.) to relative values (l.f. and yields) on the seasonal level
        for i in range(len(last_days_seasonal_abs_cols)):       
            ds_sales_last_days_seasonal = ds_sales_last_days_seasonal.\
            select('*', (ds_sales_last_days_seasonal[last_days_seasonal_abs_cols[i]]/ds_sales_last_days_seasonal['capacity_time_of_day'])\
                   .alias(last_days_seasonal_rel_cols[i]))
        #drop the now unnecessary columns (all but prices)
        cols_to_drop = last_days_seasonal_abs_cols[:] + ['flights_frequency_time_of_day', 'capacity_time_of_day']
        ds_sales_last_days_seasonal = ds_sales_last_days_seasonal.drop(*cols_to_drop)
        cols_to_drop = last_days_airport_pair_abs_cols[:] + ['flights_frequency_airport_pair', 'capacity_airport_pair']
        ds_sales_last_days_airport_pair = ds_sales_last_days_airport_pair.drop(*cols_to_drop)
        ds_sales_last_days_seasonal = ds_sales_last_days_seasonal.drop('days_in_advance')
        ds_sales_last_days_airport_pair = ds_sales_last_days_airport_pair.drop('days_in_advance')
        #rename price column names (or do it in SQL!)
        ds_sales_last_days_seasonal = ds_sales_last_days_seasonal.withColumnRenamed('price_tickets_seasonal_last006',
                                                                                    'price_tickets_typical_last006')
        ds_sales_last_days_seasonal = ds_sales_last_days_seasonal.withColumnRenamed('price_tickets_seasonal_last013',
                                                                                    'price_tickets_typical_last013')
        ds_sales_last_days_seasonal = ds_sales_last_days_seasonal.withColumnRenamed('price_tickets_seasonal_last031',
                                                                                    'price_tickets_typical_last031')
        ds_sales_last_days_seasonal = ds_sales_last_days_seasonal.withColumnRenamed('price_tickets_seasonal_last062',
                                                                                    'price_tickets_typical_last062')
        if verbose:
            logger.info('clean_sales_last_days_data() end')
    except Exception:
        logger.exception("Fatal error in clean_sales_last_days_data()")
        raise
    return(ds_sales_last_days_seasonal,
           ds_sales_last_days_airport_pair)
