# -*- coding: utf-8 -*-
"""
Created in 2018
@author: aszewczyk
Function that cleans competitors' prices data as follows:
    - if given flight has a competing flight(s) in period +/- 2 hours (avg) price obtained from this/these flight(s) is used
    - if not price from competing flight(s) in period +/- 6 hours is used
    - if not price from competing flight(s) during whole day is used
    - if not VY price are used (is it not too correlated?)
    - also competition similarity coefficient is calculated (1 if exist flights in period +/- 2 hours, 
    0.5 +/- 6 hours, 0.25 if exist flight(s) during whole day, 0.125 otherwise) 
Input:
    @demand_data - model data before cleaning
    @named_variables_lists - lists with variables used for demand forecast
 		@verbose - should print logger messages on the screen and save them to .log file?
		@logger - logger connection
Returns:
    @demand_data - model data after cleaning
"""
from  pyspark.sql.functions import when, lit, expr
def clean_competition_data(demand_data,
                           named_variables_lists,
                           verbose,
                           logger):
    try:
        if verbose:
            logger.info('clean_competition_data() start')
        #define relevant columns
        competition_2h_cols = named_variables_lists['competition_2h_cols']
        competition_6h_cols = named_variables_lists['competition_6h_cols']
        competition_day_cols = named_variables_lists['competition_day_cols']
        prices_vy_cols = named_variables_lists['prices_vy_cols']
        #add competition similarity coefficient column and set it to 1
        demand_data = demand_data.select(expr("*"), lit(1).alias("competition_similarity"))
        #first replace Null values in all competitor prices columns with 0s 
        for i in range(len(competition_2h_cols)): 
            demand_data = demand_data.fillna({competition_2h_cols[i]:0})       
        for i in range(len(competition_6h_cols)): 
            demand_data = demand_data.fillna({competition_6h_cols[i]:0})   
        for i in range(len(competition_day_cols)): 
            demand_data = demand_data.fillna({competition_day_cols[i]:0})
        #calculate competition similarity coefficient (look in function desriptions)
        demand_data = demand_data.withColumn('competition_similarity', when((demand_data['avg_price_ticket_2h_competition_007_013'] <= 0),
                                                                                                      lit(0.5)).otherwise(lit(1)))
        #if competitor price in +/- 2 hours around the given flight is Null/0 (meaning no flights), replace with price in +/- 6 hours
        for i in range(len(competition_2h_cols)): 
            demand_data = demand_data.withColumn(competition_2h_cols[i], when((demand_data[competition_2h_cols[i]] <= 0),
                                                                                                      demand_data[competition_6h_cols[i]]).
                                                                     otherwise(demand_data[competition_2h_cols[i]]))
        #update competition similarity coefficient 
        demand_data = demand_data.withColumn('competition_similarity', when((demand_data['avg_price_ticket_2h_competition_007_013'] <= 0),
                                                                                                      lit(0.25)).otherwise(lit(0.5)))
        #if competitor price is still Null or 0 (meaning no flights in +/- 6 h around the flight), replace with compt. price from whole day
        for i in range(len(competition_2h_cols)): 
            demand_data = demand_data.withColumn(competition_2h_cols[i], when((demand_data[competition_2h_cols[i]] <= 0),
                                                                                                      demand_data[competition_day_cols[i]]).
                                                                     otherwise(demand_data[competition_2h_cols[i]]))
        #update competition similarity coefficient 
        demand_data = demand_data.withColumn('competition_similarity', when((demand_data['avg_price_ticket_2h_competition_007_013'] <= 0),
                                                                                                      lit(0.125)).otherwise(lit(0.25)))
        #if competitor price is still Null or 0 (meaning no flights on the same day), replace with VY price
        for i in range(len(competition_2h_cols)): 
            demand_data = demand_data.withColumn(competition_2h_cols[i], when((demand_data[competition_2h_cols[i]] <= 0),
                                                                                                      demand_data[prices_vy_cols[i]]).
                                                                     otherwise(demand_data[competition_2h_cols[i]]))
        #drop now unneccesary columns
        demand_data = demand_data.drop(*(competition_6h_cols + competition_day_cols))
        if verbose:
            logger.info('clean_competition_data() end')
    except Exception:
        logger.exception("Fatal error in clean_competition_data()")
        raise
    return(demand_data)
