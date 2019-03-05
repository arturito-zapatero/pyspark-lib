# -*- coding: utf-8 -*-
"""
Created in 2018
@author: aszewczyk
Function that calculates conversion rates and pax from views using look to book variables in the following steps:
- using historical data the conversion rate for each segment in each of the ndo ranges is calculated as
     (conversion rate in given ndo range) = (views in this ndo range) / (pax seat sold at ndo 0)
- using calculated conversion rates calculate pax from views for each particular flight as
     (pax from views) = (views for this flight in given ndo range) * (conversion rate in given ndo range) 
Input:
    @demand_data - model data with look to book variables
    @named_variables_lists - lists with variables used for demand forecast
	@verbose - should print logger messages on the screen and save them to .log file?
		@logger - logger connection
Returns:
    @demand_data - model data with pax from views variables
TODO:  divide the Looks by the flight frequency to get the looks per flight, which is mouch more correlated with target variables ??
"""
from  pyspark.sql.functions import when, avg, sum
def calc_conversion_rates(demand_data, 
                          named_variables_lists,
                          verbose,
                          logger):
    try:
        if verbose:
            logger.info('Calculate conversion rates from L2B variables and calculate pax from views, start, function calc_conversion_rates()')
        #define relevant column names
        cumul_prod_view_cols = named_variables_lists['cumul_prod_view_cols']
        sum_cumul_prod_view_cols = named_variables_lists['sum_cumul_prod_view_cols']
        cumul_views_per_pax_cols = named_variables_lists['cumul_views_per_pax_cols']
        cumul_pax_from_views_cols = named_variables_lists['cumul_pax_from_views_cols']
        #calculate sum aggregated per airport_pair for each column
        conversion_rates = demand_data.groupBy("cd_airport_pair")\
                                  .agg(sum('pax_seat'), sum('cum_product_view_007'), sum('cum_product_view_014'), sum('cum_product_view_032'),
                                       sum('cum_product_view_045'), sum('cum_product_view_063'), sum('cum_product_view_093'), sum('cum_product_view_120'))\
                                  .withColumnRenamed('sum(pax_seat)', 'sum_pax_seat')
        #rename column names (e.g. 'sum(cum_product_view_007)' to 'sum_cum_product_view_007')
        for i in range(len(cumul_prod_view_cols)):
            conversion_rates = conversion_rates.withColumnRenamed('sum('+cumul_prod_view_cols[i]+')', 'sum_'+cumul_prod_view_cols[i])
        #calculate views per pax seat sold (ratio of how many clients that look at the product at given ndo did actually bought ticket)
        for i in range(len(cumul_prod_view_cols)):
            conversion_rates = conversion_rates.select('*', (conversion_rates[sum_cumul_prod_view_cols[i]]/conversion_rates['sum_pax_seat']).alias(cumul_views_per_pax_cols[i]))
        conversion_rates.cache()
        #replace rows with Nulls in each column (ie. given ndo) with the average for this column. Values smaller than 0.001 are set to 0.001 (to avoid outliers/bad data)
        for i in range(len(cumul_prod_view_cols)):
            #this part is slow, optimize!
            avg_views_per_pax_tmp = conversion_rates.agg(avg(cumul_views_per_pax_cols[i])).collect()[0][0]
            conversion_rates = conversion_rates.fillna({cumul_views_per_pax_cols[i]:'avg_views_per_pax_tmp'})
            #maybe not necessary?
            conversion_rates = conversion_rates.withColumn(cumul_views_per_pax_cols[i], when((conversion_rates[cumul_views_per_pax_cols[i]] <= 0.001),
                                                                                                              0.001).
                                                                             otherwise(conversion_rates[cumul_views_per_pax_cols[i]]))
        #join conversion rates to data and calculate pax from view
        demand_data = demand_data.join(conversion_rates, ["cd_airport_pair"], "leftouter")
        for i in range(len(sum_cumul_prod_view_cols)):
            demand_data = demand_data.select('*', (demand_data[cumul_prod_view_cols[i]]/demand_data[cumul_views_per_pax_cols[i]]).alias(cumul_pax_from_views_cols[i]))
        #drop now unnecessary columns:
        for i in ['sum_pax_seat']+sum_cumul_prod_view_cols+cumul_views_per_pax_cols:
            demand_data = demand_data.drop(i)
        if verbose:
            logger.info('Calculate conversion rates from L2B variables and calculate pax from views, end')
    except Exception:
        logger.exception("Fatal error in calc_conversion_rates()")
        raise
    return(demand_data)