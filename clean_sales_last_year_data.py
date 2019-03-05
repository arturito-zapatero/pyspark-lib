# -*- coding: utf-8 -*-
"""
Created in 2018
@author: aszewczyk
Function that cleans last year sales data. Lowest aggregation level is time of day-week day-segment,
 rows with Nulls on this agg level
are replaced with data from aggragation level of weekday-segment. If still no data are available (Nulls)
 we take data agg over the whole week,
and finally agg over the whole year
- also last year similarity coefficient is calculated (1 if row have data on highest agg level,
 multiply by 0.5 for each lower agg level) 
Input:
    @ds_sales_last_year - sales last year data before cleaning
    @named_variables_lists - lists with variables used for demand forecast
 	@verbose - should print logger messages on the screen and save them to .log file?
	@logger - logger connection
Returns:
    @ds_sales_last_year - sales last year data after cleaning
"""
from  pyspark.sql.functions import lit, when, expr
def clean_sales_last_year_data(ds_sales_last_year,
                               named_variables_lists,
                               verbose,
                               logger):
    try:
        if verbose:
            logger.info('clean_sales_last_year_data() start')

        #first read the columns' names for each level of aggregation starting with DOW-TOD-segment ending with year-segment
        sales_last_year_agg1_cols = named_variables_lists['sales_last_year_agg1_cols']
        sales_last_year_agg2_cols = named_variables_lists['sales_last_year_agg2_cols']
        sales_last_year_agg3_cols = named_variables_lists['sales_last_year_agg3_cols']
        sales_last_year_agg4_cols = named_variables_lists['sales_last_year_agg4_cols']
        #second step change NAs/Nulls to 0s in pax data, now each row with 0 is the row with missing data for given aggregation
        #besides the lowest aggregation level: year
        #pax variable will be used as Null reference (line where pax_seat is Null will have the other columns like revenue replaced even if they are not Nulls!)
        ds_sales_last_year = ds_sales_last_year.fillna({'sum_pax_seat_time_of_day_prev_year':0})
        ds_sales_last_year = ds_sales_last_year.fillna({'sum_pax_seat_day_of_week_prev_year':0})
        ds_sales_last_year = ds_sales_last_year.fillna({'sum_pax_seat_week_prev_year':0})
        #create similarity coefficient between the c.y. flight and the l.y. flights and set it to one
        ds_sales_last_year = ds_sales_last_year.select(expr("*"), lit(1).alias("last_year_similarity"))
        #update similarirty coefficient (ie. rows with no data on the highest agg level will have it lowered to 0.5, with data stays at 1)
        ds_sales_last_year = ds_sales_last_year.withColumn('last_year_similarity', when((ds_sales_last_year['sum_pax_seat_time_of_day_prev_year'] <= 0),
                                                                                                      ds_sales_last_year['last_year_similarity']*0.5).
                                                           otherwise(ds_sales_last_year['last_year_similarity']))
        #if no last year data on highest aggregation level replace with lower aggregation level data
        #loop for each variable, pax variable at the end as it is our reference Null variable. 
        for i in range(len(sales_last_year_agg1_cols)):
            ds_sales_last_year = ds_sales_last_year.withColumn(sales_last_year_agg1_cols[i], when((ds_sales_last_year.sum_pax_seat_time_of_day_prev_year <= 0),
                                                                               ds_sales_last_year[sales_last_year_agg2_cols[i]]).
                                                    otherwise(ds_sales_last_year[sales_last_year_agg1_cols[i]]))
        #update similarirty coefficient
        ds_sales_last_year = ds_sales_last_year.withColumn('last_year_similarity', when((ds_sales_last_year['sum_pax_seat_day_of_week_prev_year'] <= 0),
                                                                                                      ds_sales_last_year['last_year_similarity']*0.5).
                                                           otherwise(ds_sales_last_year['last_year_similarity']))
        #if still no last year data replace with lower aggregation level data
        for i in range(len(sales_last_year_agg2_cols)):    
            ds_sales_last_year = ds_sales_last_year.withColumn(sales_last_year_agg1_cols[i], when((ds_sales_last_year.sum_pax_seat_time_of_day_prev_year <= 0),
                                                                               ds_sales_last_year[sales_last_year_agg3_cols[i]]).
                                                    otherwise(ds_sales_last_year[sales_last_year_agg1_cols[i]]))
        #update similarity coefficient
        ds_sales_last_year = ds_sales_last_year.withColumn('last_year_similarity', when((ds_sales_last_year['sum_pax_seat_week_prev_year'] <= 0),
                                                                                                      ds_sales_last_year['last_year_similarity']*0.5).
                                                           otherwise(ds_sales_last_year['last_year_similarity']))
        #if still no last year data replace with lower aggregation level data (segment - year)
        for i in range(len(sales_last_year_agg3_cols)):
            ds_sales_last_year = ds_sales_last_year.withColumn(sales_last_year_agg1_cols[i], when((ds_sales_last_year.sum_pax_seat_time_of_day_prev_year <= 0),
                                                                               ds_sales_last_year[sales_last_year_agg4_cols[i]]).
                                                    otherwise(ds_sales_last_year[sales_last_year_agg1_cols[i]]))
        #remove duplicates/columns not used for join
        ds_sales_last_year = ds_sales_last_year.limit(ds_sales_last_year.count()).drop('id_season', 'dt_flight_date_local')
        #drop now unneccesary columns
        ds_sales_last_year = ds_sales_last_year.drop(*(sales_last_year_agg2_cols + sales_last_year_agg3_cols + sales_last_year_agg4_cols))
        #now the na's are for the non exisitng flights only - can be dropped here, anyway we left join to demand_data
        ds_sales_last_year = ds_sales_last_year.dropna(how='any')
        if verbose:
            logger.info('clean_sales_last_year_data() end')
    except Exception:
        logger.exception("Fatal error in clean_sales_last_year_data()")
        raise
    return(ds_sales_last_year)
