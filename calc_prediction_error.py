# -*- coding: utf-8 -*-
"""
Created in 022019
@author: aszewczyk
Function that adds error (SE, APE and AE) columns to spark df with model results, also evaluates model error (MAPE and APE) in a given period of time
Input:
    From number_of_last_days_to_eval, last_dt_exec_to_evaluate, list_exec_dates_to_evalute only one mtric should be set, other should be False
    @number_of_last_days_to_eval - evaluates number_of_last_days_to_eval days, takes the first model with dt_execution > today - number_of_last_days_to_eval
    @last_dt_exec_to_evaluate - integer, number of last model execution date to evaluate
    @list_exec_dates_to_evalute - list with strings of execution dates (format yyyy-mm-dd) to evaluate error
    @remove_outliers - should remove outliers (flights with l.f. < 25% (lowest 1 decile) and yield per pax < 15 euros)
    @verbose - should print logger messages on the screen and save them to .log file?
	@logger - logger connection
    @checks - should calculate advanced error metrics
Returns:
    @df_prediction_errors - spark df with model results with added error columns for three target variables, flight level up to today
    @pred_errors - dictionary with prediction evaluation metrics
"""
from add_error_cols import add_error_cols
from create_spark_session import create_spark_session
from datetime import datetime, timedelta
from pyspark.sql.functions import col
def calc_prediction_error(number_of_last_days_to_eval=90,
                          last_dt_exec_to_evaluate=False,
                          list_exec_dates_to_evalute=False,
                          remove_outliers=True,
                          verbose=True,
                          logger=False,
                          checks=True
                          ):

    try:
        if verbose:
            logger.info('calc_prediction_error() start')
        spark = create_spark_session(jarra='quinto',
                                     verbose=verbose,
                                     logger=logger)
        today_date = datetime.today().strftime('%Y-%m-%d')
        #get real data
        ds_real_data   = spark.sql("""SELECT *, pax_seat/seats as capacity, revenue_tickets/pax_seat as yield_tickets FROM revenue.ds_flight_master""")\
                              .select('dt_flight_date_local', 'cd_airport_pair', 'cd_num_flight', 'cd_carrier',
                                      'revenue_tickets', 'pax_seat', 'ancis', 'revenue_total', 'capacity', 'yield_tickets')
        #rename columns
        oldColumns = ["dt_flight_date_local", "cd_num_flight", "cd_carrier"]
        newColumns = ['dt_flight_date_local_zone', 'cd_flight_number', 'cd_airline_code']
        ds_real_data = reduce(lambda ds_real_data, idx: ds_real_data.withColumnRenamed(oldColumns[idx], newColumns[idx]), xrange(len(oldColumns)), ds_real_data)
        #get predictions
        ds_predictions = spark.sql("""SELECT * FROM revenue.ds_demand_forecast_results""")\
                              .select('dt_flight_date_local_zone', 'cd_airport_pair', 'cd_flight_number', 'cd_airline_code',
                                   'target_pax_seat', 'target_revenue_tickets', 'target_ancis', 'dt_execution')\
                              .dropna(how='any', subset=['target_pax_seat', 'target_revenue_tickets', 'target_ancis'])
        #join predictions with real values, only up to today
        ds_full = ds_predictions\
                  .join(ds_real_data, ["dt_flight_date_local_zone", "cd_airport_pair", "cd_flight_number", "cd_airline_code"], "leftouter")\
                  .filter(col('dt_flight_date_local_zone')<today_date)
        #drop outliers (flights with l.f. below 25% (lowest 1 decile))
        if remove_outliers:
            ds_full = ds_full\
                      .filter(col('capacity')>.25)\
                      .filter(col('yield_tickets')>15)
        ds_full = ds_full\
                  .drop(*['capacity', 'yield_tickets'])
        #add error columns for each of target variables
        df_prediction_errors = add_error_cols(ds_full, 
                                   col_target='revenue_tickets',
                                   col_predict='target_revenue_tickets',
                                   verbose=False,
                                   logger=False)
        df_prediction_errors = add_error_cols(df_prediction_errors, 
                                   col_target='pax_seat',
                                   col_predict='target_pax_seat',
                                   verbose=False,
                                   logger=False)
        df_prediction_errors = add_error_cols(df_prediction_errors, 
                                   col_target='ancis',
                                   col_predict='target_ancis',
                                   verbose=False,
                                   logger=False)
        df_prediction_errors.cache()
        if checks:
            #get model exec dates for error evaluation printed in logger
            unique_exec_dates = df_prediction_errors\
                                .select('dt_execution')\
                                .distinct()\
                                .sort(col('dt_execution').desc())\
                                .toPandas()['dt_execution']\
                                .tolist()
            if number_of_last_days_to_eval:
                eval_start_date = (datetime.today() - timedelta(days=number_of_last_days_to_eval)).date()
                list_exec_dates_to_evalute = [dates for dates in unique_exec_dates if dates > eval_start_date][-1]
            elif last_dt_exec_to_evaluate:
                list_exec_dates_to_evalute = unique_exec_dates[0:last_dt_exec_to_evaluate]   
            #get only model executed on list_exec_dates_to_evalute
            df_prediction_errors_filtered = df_prediction_errors.filter(col('dt_execution').isin(list_exec_dates_to_evalute))
            if df_prediction_errors_filtered.count() == 0 and verbose:
                logger.error('No data for evaluation, probably no model were executed for given execution dates list')
            #calculate error for given dt_execution
            revenueTicketsMAPE = df_prediction_errors_filtered\
                                .agg({'revenue_tickets_APE': 'avg'})\
                                .toPandas()\
                                .iloc[0,0]
            revenueTicketsMAE = df_prediction_errors_filtered\
                                .agg({'revenue_tickets_AE': 'avg'})\
                                .toPandas()\
                                .iloc[0,0]
            paxSeatMAPE = df_prediction_errors_filtered\
                            .agg({'pax_seat_APE': 'avg'})\
                            .toPandas()\
                            .iloc[0,0]
            paxSeatMAE = df_prediction_errors_filtered\
                            .agg({'pax_seat_AE': 'avg'})\
                            .toPandas()\
                            .iloc[0,0]
            revenueAncisMAPE = df_prediction_errors_filtered\
                                .agg({'ancis_APE': 'avg'})\
                                .toPandas()\
                                .iloc[0,0]
            revenueAncisMAE = df_prediction_errors_filtered\
                                .agg({'ancis_AE': 'avg'})\
                                .toPandas()\
                                .iloc[0,0]
            if remove_outliers:
                logger.info('Outlier Flights (LF < 25% and yield per pax < 15) were excluded from evaluation')
            if verbose:
                logger.info('MAPE for revenue tickets for dates: ' + str(list_exec_dates_to_evalute) + ' is ' + str(round(revenueTicketsMAPE,2)) + '%')
                logger.info('MAE for revenue tickets for dates: '  + str(list_exec_dates_to_evalute) + ' is ' + str(round(revenueTicketsMAE,2)) + ' euros')
                logger.info('MAPE for pax seat for dates: ' + str(list_exec_dates_to_evalute) + ' is ' + str(round(paxSeatMAPE,2)) + '%')
                logger.info('MAE for pax seat for dates: '  + str(list_exec_dates_to_evalute) + ' is ' + str(round(paxSeatMAE,2)) + ' pax')
                logger.info('MAPE for revenue ancis for dates: ' + str(list_exec_dates_to_evalute) + ' is ' + str(round(revenueAncisMAPE,2)) + '%')
                logger.info('MAE for revenue ancis for dates: '  + str(list_exec_dates_to_evalute) + ' is ' + str(round(revenueAncisMAE,2)) + ' euros')
                logger.info('calc_prediction_error() end')
            pred_errors = {'revenueTicketsMAPE': revenueTicketsMAPE, 
                           'revenueTicketsMAE': revenueTicketsMAE,
                           'paxSeatMAPE': paxSeatMAPE,
                           'paxSeatMAE': paxSeatMAE, 
                           'revenueAncisMAPE': revenueAncisMAPE,
                           'revenueAncisMAE': revenueAncisMAE
                        }
        else:
            pred_errors = {}
    except Exception:
        logger.exception("Fatal error in calc_prediction_error()")
        raise
    return(df_prediction_errors, pred_errors)