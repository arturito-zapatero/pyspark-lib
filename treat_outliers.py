# -*- coding: utf-8 -*-
"""
Created in 2018
@author: aszewczyk
Function that removes outliers from the model data based on min and max lf, yield_ticket and prices.
For model in evaluation removes outliers from whole data set, for model predictions
 (ie. model in production) only remove outliers from training set (ie. until today)
Input:
		@data - pandas df with model data to remove outliers
    @named_variables_lists - lists with variables used for demand forecast
    @model_evaluation - if True model is evaluated, if False model is used for prediction
    @split_value - date for the first day of predictions/evaluation (for predictions only 
    data until before this date have outliers removed)
		@min_lf - min load factor threshold, rows where LF is lower than this threshold will 
        be removed from training data
		@min_yield - as above, but yield tickets
		@max_lf - max load factor threshold, rows where LF is higher than this threshold will
        be removed from training data
		@max_price - analogously as above but with prices
		@max_yield - analogously as above but with yields
    @verbose - should print logger messages on the screen and save them to .log file?
		@logger - logger connection
	Returns:
		@fullData - pandas df with model data with outliers removed
TODO: for now doesn't use ancis to get rid of outlier flights, implement later
"""
from  pyspark.sql.functions import col
def treat_outliers(data,
                   named_variables_lists,
                   model_evaluation,
                   split_value,
				   min_lf=0,
				   min_yield=0,
		           max_lf=1.5,
				   max_price=500,
				   max_yield=500,
                   max_yield_ancis=50,
                   verbose=False,
                   logger=False):
    try:
        if verbose:
            logger.info('Remove outliers start, function treat_outliers()')
        #last_day_data = end_days_list[len(end_days_list)-1]
          #define outliers' affected columns:
        yield_tickets_cols = named_variables_lists['yield_tickets_cols']
        #yield_ancis_cols = named_variables_lists['yield_ancis_cols']
        lf_cols = named_variables_lists['lf_cols']
        price_cols = named_variables_lists['price_cols']

        if model_evaluation:
            if verbose:
                logger.info('For model evaluation outliers removed in training and test data sets')
            #remove ouliers from full data set
            fullData = data
            for i in range(len(yield_tickets_cols)):
                yieldOutlierFilter = col(yield_tickets_cols[i]) < max_yield
                fullData = fullData.where(yieldOutlierFilter)

            for i in range(len(price_cols)):
                priceOutlierFilter = col(price_cols[i]) < max_price
                fullData = fullData.where(priceOutlierFilter)

            for i in range(len(lf_cols)):
                lfOutlierFilter = col(lf_cols[i]) < max_lf
                fullData = fullData.where(lfOutlierFilter)

            minRevenueFilter = col('revenue_tickets') > 0
            minPaxFilter = col('pax_seat') > 0
            minLFFilter = col('lf') > min_lf
            minYieldFilter = col('yield_tickets') > min_yield
            fullData = fullData.where(minRevenueFilter)
            fullData = fullData.where(minPaxFilter)
            fullData = fullData.where(minLFFilter)
            fullData = fullData.where(minYieldFilter)
        else:
            #remove outliers only from training set (until split_value)
            if verbose:
                logger.info('For predictions outliers removed only in training data set')
            trainDatesFilter = col("dt_flight_date_local") < split_value
            testDatesFilter = col("dt_flight_date_local") >= split_value
            trainData = data.where(trainDatesFilter)
            testData = data.where(testDatesFilter)
            for i in range(len(yield_tickets_cols)):
                yieldOutlierFilter = col(yield_tickets_cols[i]) < max_yield
                trainData = trainData.where(yieldOutlierFilter)

            for i in range(len(price_cols)):
                priceOutlierFilter = col(price_cols[i]) < max_price
                trainData = trainData.where(priceOutlierFilter)

            for i in range(len(lf_cols)):
                lfOutlierFilter = col(lf_cols[i]) < max_lf
                trainData = trainData.where(lfOutlierFilter)
            minRevenueFilter = col('revenue_tickets') > 0
            minPaxFilter = col('pax_seat') > 0
            minLFFilter = col('lf') > min_lf
            minYieldFilter = col('yield_tickets') > min_yield
            trainData = trainData.where(minRevenueFilter)
            trainData = trainData.where(minPaxFilter)
            trainData = trainData.where(minLFFilter)
            trainData = trainData.where(minYieldFilter)

            fullData = trainData.union(testData)
        if verbose:
            logger.info('Remove outliers end')
    except Exception:
        logger.exception("Fatal error in treat_outliers()")
        raise
    return(fullData)
