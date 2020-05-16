# -*- coding: utf-8 -*-
"""
Created in 2018
@author: aszewczyk
Function that evaluates model errors (AE and APE) on day, month, month-segment levels
Input:
    @model_results_spk - spark data frame with model's results  
    @col_target - string with name of target column
    @col_predict - string with name of prediction column
    @model_evaluation - 
        basic: only basic error metrics are calculated
        full: extra error metrics, also feature importance, are calculated
    @eval_start - error evaluation will start from this day
    @col_agg1, @col_agg2 - strings with aggregation levels to calcualte errors
    @verbose - should print logger messages on the screen and save them to .log file?
    @logger - logger connection
Returns:
    @model_results_spk - spark data frame with model's results with added error columns
TODO: generalize function so it has any number of aggregation level (passed as list possbibly), clean the code

"""
from  pyspark.sql.functions import col, abs, sum, count, avg, abs
def modelErrorsEval(model_results_spk,
                    col_target,
                    col_predict,
                    model_evaluation,
                    eval_start,
                    col_agg1,
                    col_agg2,
                    verbose,
                    logger):
    try:
        if verbose:
            logger.info('Evaluatian of model errors start, evaluation levels: ' + model_evaluation + ' , function demand_forecast_model_eval()')
        model_results_spk = model_results_spk.filter(col('dt_flight_date_local') >= eval_start)

        # Evaluation on day level
        model_results_spk = model_results_spk\
                        .select('*', abs((model_results_spk[col_target] - model_results_spk[col_predict])
                                     /model_results_spk[col_target]*100)\
                    .alias(col_target+'_APE'))
        model_results_spk = model_results_spk\
                        .select('*', abs((model_results_spk[col_target] - model_results_spk[col_predict])
                                     )\
                        .alias(col_target+'_AE'))
        if verbose:
            logger.info('MAPE is ' +
                  str(round(model_results_spk.agg({col_target+'_APE': 'avg'}).collect()[0][0], 3)) + ' %')
            logger.info('MAE  is ' +
                  str(round(model_results_spk.agg({col_target+'_AE': 'avg'}).collect()[0][0], 3)) + ' ')

            logger.info('Outliers (APE > 50%)) part is ' +
                  str(round(model_results_spk.filter(col(col_target+'_APE') > 50)\
                            .count()*100.00/model_results_spk.count(), 2)) + '%')
            logger.info('Outliers (APE > 100%)) part is ' +
                  str(round(model_results_spk.filter(col(col_target+'_APE') > 100)\
                            .count()*100.00/model_results_spk.count(), 2)) + '%')

        agg_day_segment = model_results_spk.groupBy(col_agg1, col_agg2)\
                               .agg({col_target: 'sum', col_predict: 'sum'})
        agg_day_segment = agg_day_segment.select('*', abs(agg_day_segment['sum('+col_target+')'] -
                                                          agg_day_segment['sum('+col_predict+')'])\
                                   .alias('daily_abs_error'))
        agg_day_segment = agg_day_segment.select('*', abs((agg_day_segment['sum('+col_target+')'] -
                                                           agg_day_segment['sum('+col_predict+')'])
                                           /agg_day_segment['sum('+col_target+')']*100)\
                           .alias('daily_abs_perc_error'))

        if verbose:
            logger.info('MAE agg on  level is ' +
                     str(round(agg_day_segment.agg({'daily_abs_error': 'avg'}).collect()[0][0], 3)))
            logger.info('MAPE agg on level is ' +
                     str(round(agg_day_segment.agg({'daily_abs_perc_error': 'avg'}).collect()[0][0], 3)) + ' %')

        # Evaluation on month-segment level
        agg_month_segment = model_results_spk.groupBy(col_agg1, col_agg2)\
                         .agg({col_target: 'sum', col_predict: 'sum'})
        agg_month_segment = agg_month_segment.select('*', abs(agg_month_segment['sum('+col_target+')'] -
                                                              agg_month_segment['sum('+col_predict+')'])\
                                      .alias('monthly_abs_error'))
        agg_month_segment = agg_month_segment.select('*', abs((agg_month_segment['sum('+col_target+')'] -
                                                               agg_month_segment['sum('+col_predict+')'])
                                               /agg_month_segment['sum('+col_target+')']*100)\
                               .alias('monthly_abs_perc_error'))            

        if verbose:
            logger.info('MAE agg on x level is ' +
                     str(round(agg_month_segment.agg({'monthly_abs_error': 'avg'}).collect()[0][0], 3)))


            logger.info('MAPE agg on x level is ' +
                     str(round(agg_month_segment.agg({'monthly_abs_perc_error': 'avg'}).collect()[0][0], 3)) + ' %')

        # Calculate extra evaluation metrics
        if model_evaluation == 'full':

            # Evaluation on day level
            agg_day = model_results_spk.groupBy(col_agg1)\
                       .agg({col_target: 'sum', col_predict: 'sum'})
            agg_day = agg_day.select('*', abs(agg_day['sum('+col_target+')'] - agg_day['sum('+col_predict+')'])\
                                       .alias('daily_abs_error'))
            agg_day = agg_day.select('*', abs((agg_day['sum('+col_target+')'] - agg_day['sum('+col_predict+')'])
                                               /agg_day['sum('+col_target+')']*100)\
                               .alias('daily_abs_perc_error'))
            if verbose:
                logger.info('MAE agg on daily level is ' +
                         str(round(agg_day.agg({'daily_abs_error': 'avg'}).collect()[0][0], 3)))
                logger.info('MAPE agg on daily level is ' +
                         str(round(agg_day.agg({'daily_abs_perc_error': 'avg'}).collect()[0][0], 3)) + ' %')

            # Evaluation on month level
            agg_month = model_results_spk.groupBy(col_agg2)\
                             .agg({col_target: 'sum', col_predict: 'sum'})
            agg_month = agg_month.select('*', abs(agg_month['sum('+col_target+')'] - agg_month['sum('+col_predict+')'])\
                                          .alias('monthly_abs_error'))
            agg_month = agg_month.select('*', abs((agg_month['sum('+col_target+')'] - agg_month['sum('+col_predict+')'])
                                                   /agg_month['sum('+col_target+')']*100)\
                                   .alias('monthly_abs_perc_error'))
            if verbose:
                logger.info('MAE agg on monthly level is ' +
                         str(round(agg_month.agg({'monthly_abs_error': 'avg'}).collect()[0][0], 3)))
                logger.info('MAPE agg on monthly level is ' +
                         str(round(agg_month.agg({'monthly_abs_perc_error': 'avg'}).collect()[0][0], 3)) + ' %')
        if verbose:
            logger.info('Evaluatian of model errors end')
    except Exception:
        logger.exception("Fatal error in demand_forecast_model_eval()")
        raise
    return model_results_spk
