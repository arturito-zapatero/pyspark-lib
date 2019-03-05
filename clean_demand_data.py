# -*- coding: utf-8 -*-
"""
Created in 2018
@author: aszewczyk
Function that:
- replace NULLs and 0s in typical last days sales data with with yearly averaged data
- calculate coefficient similarity strength of the typical last days variables 
- replace Null values in prices (flights with no sales in given lag) with minimum price for given flight
- replace Nulls in holiday, season variables with 0s
- flights without assigned clusters are assigned a 'synthetic' one ('999')
- convert absolute values to relative values in lag variables and last year variables 
(ie. pax_seat to lf, revenue to yield per pax, ancis to yield_ancis)
Input:
    @demand_data - model data to be cleaned
    @named_variables_lists - lists with variables used for demand forecast
    @verbose - should print logger messages on the screen and save them to .log file?
		@logger - logger connection
Returns:
    @demand_data - model data after cleaning
"""
from  pyspark.sql.functions import when, lit
def clean_demand_data(demand_data,
                      named_variables_lists,
                      verbose,
                      logger):
    try: 
        if verbose:
            logger.info('Cleaning demand data start, function clean_demand_data()')
        #we first deal with Nulls in (typical sales) last days variables
        #rows for last days variables with Null values on seasonal agg level (defined by DOW, TOD, season, segment) 
        #are replaced with segment agg level (ie. averaged last days sales data from all flights for this segemnt in last year)

        #last_days_airport_pair_abs_cols = named_variables_lists['last_days_airport_pair_abs_cols'] #delete?
        #last_days_seasonal_abs_cols = named_variables_lists['last_days_seasonal_abs_cols'] #delete?
        #define columns for last days data
        last_days_airport_pair_rel_cols = named_variables_lists['last_days_airport_pair_rel_cols'] + \
        ['price_tickets_airport_pair_last006', 'price_tickets_airport_pair_last013', 'price_tickets_airport_pair_last031', 'price_tickets_airport_pair_last062']
        last_days_seasonal_rel_cols = named_variables_lists['last_days_seasonal_rel_cols'] + \
        ['price_tickets_typical_last006', 'price_tickets_typical_last013', 'price_tickets_typical_last031', 'price_tickets_typical_last062']

        #1. Replace NULLs and 0s in typical last days sales data with with yearly averaged data. First set Nulls to 0s, than replace all 0s.
        #These NULLs are for flights with no flights in the last year for given segment-DOW-TOD
        for i in range(len(last_days_seasonal_rel_cols)): 
            demand_data = demand_data.fillna({last_days_seasonal_rel_cols[i]:0})
        #calculate coefficient similarity strength of the last days variables (if based on seasonal agg. is 1, 
        #if this is missing and we have to take to airport pair agg. we set it to 0.5)
        demand_data = demand_data.withColumn('last_days_similarity', when((demand_data['lf_typical_last006'] <= 0),
                                                                                                      lit(0.5)).otherwise(lit(1)))
        for i in range(len(last_days_seasonal_rel_cols)):                                           
            demand_data = demand_data.withColumn(last_days_seasonal_rel_cols[i], when((demand_data[last_days_seasonal_rel_cols[i]] <= 0),
                                                                                                      demand_data[last_days_airport_pair_rel_cols[i]]).
                                                                     otherwise(demand_data[last_days_seasonal_rel_cols[i]]))
        #drop now unneccesary columns 
        demand_data = demand_data.drop(*last_days_airport_pair_rel_cols)
        #2. replace Null values in prices (flights with no sales in given lag) with minimum price for given flight
        lag_price_cols = named_variables_lists['lag_price_cols']
        for i in range(len(lag_price_cols)):
            demand_data = demand_data.fillna({lag_price_cols[i]:'min_price_ticket_segment'})
            demand_data = demand_data.withColumn(lag_price_cols[i], when((demand_data[lag_price_cols[i]].isNull()),
                                                                               demand_data['min_price_ticket_segment']).
                                                    otherwise(demand_data[lag_price_cols[i]]))
            #if still lagged price = 0 (when 'min_price_ticket_segment' do not exists: e.g. for larger lags and new flights) then replace with 20 euro
            #this should be rethinked
            demand_data = demand_data.fillna({lag_price_cols[i]:'20'})									
        #3. replace Nulls in holiday, season variables with 0s
        demand_data = demand_data.fillna({'is_event_orig':'0'})
        demand_data = demand_data.fillna({'is_event_dest':'0'})
        demand_data = demand_data.fillna({'id_season':'0'})
        #4. flights without assigned clusters are assigned a 'synthetic' one ('999')
        demand_data = demand_data.fillna({'cluster':'999'})
        demand_data = demand_data.fillna({'cluster_afp':'999'})
        #5.convert absolute values to relative values in lag variables (ie. pax_seat to lf, revenue to yield per pax, ancis to yield_ancis)
        #define column names
        cumul_pax_cols = named_variables_lists['cumul_pax_cols']
        cumul_lf_cols = named_variables_lists['cumul_lf_cols']
        cumul_revenue_ticket_cols = named_variables_lists['cumul_revenue_ticket_cols']
        cumul_yield_ticket_cols = named_variables_lists['cumul_yield_ticket_cols']
        cumul_revenue_ancis_cols = named_variables_lists['cumul_revenue_ancis_cols']
        cumul_yield_ancis_cols = named_variables_lists['cumul_yield_ancis_cols']
        cumul_revenue_total_cols = named_variables_lists['cumul_revenue_total_cols']
        cumul_yield_total_cols = named_variables_lists['cumul_yield_total_cols']
        last_year_abs_seasonal_cols = named_variables_lists['last_year_abs_seasonal_cols']
        last_year_rel_seasonal_cols = named_variables_lists['last_year_rel_seasonal_cols']
        sales_last_year_agg1_cols = named_variables_lists['sales_last_year_agg1_cols']
        all_abs_cols = cumul_pax_cols + cumul_revenue_ticket_cols + cumul_revenue_ancis_cols #+ cumul_revenue_total_cols
        all_rel_cols = cumul_lf_cols + cumul_yield_ticket_cols + cumul_yield_ancis_cols #+ cumul_yield_total_cols
        for i in range(len(all_abs_cols)):
            demand_data = demand_data.select('*', (demand_data[all_abs_cols[i]]/demand_data['seats']).alias(all_rel_cols[i]))
        #6.convert absolute values to relative values in last year variables (ie. pax_seat to lf, revenue to yield per pax, ancis to yield_ancis)
        for i in range(len(last_year_abs_seasonal_cols)):
            demand_data = demand_data.select('*', (demand_data[last_year_abs_seasonal_cols[i]]/demand_data['sum_capacity_time_of_day_prev_year']).alias(last_year_rel_seasonal_cols[i]))
        demand_data = demand_data.withColumnRenamed('avg_price_tickets_time_of_day_prev_year', 'price_tickets_last_year')
        #cols_to_drop = sales_last_year_agg1_cols[:]
        #cols_to_drop.remove('avg_price_tickets_time_of_day_prev_year')
        demand_data = demand_data.drop(*sales_last_year_agg1_cols[:])
        if verbose:
            logger.info('Cleaning demand data end')
    except Exception:
        logger.exception("Fatal error in clean_demand_data()")
        raise
    return(demand_data)
