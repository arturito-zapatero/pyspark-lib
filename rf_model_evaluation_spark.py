# -*- coding: utf-8 -*-
"""
Created in 2018
@author: aszewczyk
Function that evaluates model error in spark by calculating MAE and MAPE on flight level and month-route level
Input:
    @transformedFull - data with RF model results 
    @col_target - string with target column name
    @col_predict - string with prediction column name
    @i_value, j_value, k_value - evaluated model h-parameters value
    @i_param, j_param, k_param - evaluated model h-parameters name
    @verbose - should print logger messages on the screen and save them to .log file?
	@logger - logger connection
Returns:
    None
"""  
from pyspark.sql.functions import abs, sum, sqrt
import numpy as np
def rf_model_evaluation_spark(transformedFull,
                        col_target,
                        col_predict,
                        i_value, j_value, k_value,
                        i_param, j_param, k_param,
                        verbose,
                        logger):
    try:
        if verbose:
            logger.info('Evaluates of RF model errors (Spark) start, function rf_model_evaluation_spark()')
        logger.info('Used parameters are: \n subsamplingRate: ' +\
        '\n ' + i_param + ': ' + str(i_value) +\
        '\n ' + j_param + ': ' + str(j_value) +\
        '\n ' + k_param + ': ' + str(k_value))
        logger.info('MAPE for ' + col_target + ' is ' +
                  str(round(transformedFull.agg({col_target+'_APE': 'avg'}).collect()[0][0], 3)) + ' %')    
        logger.info('MAE for ' + col_target + ' is ' +
                  str(round(transformedFull.agg({col_target+'_AE': 'avg'}).collect()[0][0], 3)))   
        logger.info('RMSE for ' + col_target + ' is ' +
                  str(round(np.sqrt(transformedFull.agg({col_target+'_SE': 'avg'}).collect()[0][0]), 3)))
        aggMonthRouteFull = transformedFull.groupBy('dt_flight_year_month', 'cd_airport_pair_order')\
                                .agg({col_target: 'sum', col_predict: 'sum'})\
                                .withColumnRenamed('sum('+col_target+')',  'sum_target_var_month_route')\
                                .withColumnRenamed('sum('+col_predict+')', 'sum_predict_var_month_route')
        aggMonthRouteFull = aggMonthRouteFull.select('*', abs(aggMonthRouteFull['sum_target_var_month_route'] - aggMonthRouteFull['sum_predict_var_month_route'])\
                                       .alias('month_route_AE'))
        aggMonthRouteFull = aggMonthRouteFull.select('*', abs((aggMonthRouteFull['sum_target_var_month_route'] - aggMonthRouteFull['sum_predict_var_month_route'])
                                               /aggMonthRouteFull['sum_target_var_month_route']*100)\
                               .alias('month_route_APE'))
        logger.info('MAPE agg on month-route level for ' + col_target + ' is ' +
                     str(round(aggMonthRouteFull.agg({'month_route_APE': 'avg'}).collect()[0][0], 3)) + ' %')
        logger.info('MAE agg on month-route level for ' + col_target + ' is ' +
                     str(round(aggMonthRouteFull.agg({'month_route_AE': 'avg'}).collect()[0][0], 3)))
        if verbose:
            logger.info('Evaluates of RF model errors (Spark) end')
    except Exception:
        logger.exception("Fatal error in rf_model_evaluation_spark()")
        raise
    return()
