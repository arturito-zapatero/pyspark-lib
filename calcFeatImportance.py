# -*- coding: utf-8 -*-
"""
Created in 2019
@author: aszewczyk
Function that calculates feature importance for first and last model (ie. for next week and for third month), joins them in
one df, which can e.g. be saved in S3. Only models with full features lists
Input:
    @fitList - list with model (e.g. rf) pipelines for each consecutive model with all features
    @testDataList - list with data for prediction for each consecutive model with all features
    @col_target - string with target column name
    @verbose - should print logger messages on the screen and save them to .log file?
    @first_pred_day - first day of predictions (ie. for production tomorrow)
	@logger - logger connection
Returns:
    @transformedFull -  - spark df with model results with added error columns
"""
from rf_feature_importance import rf_feature_importance
def calcFeatImportance(fitList,
                       testDataList,
                       col_target,
                       first_pred_day,
                       verbose,
                       logger):
    try:
        if verbose:
            logger.info('calc_feat_importance_all step start')
        fittedDataFirst = fitList[0]
        transformedDataFirst = testDataList[0]
        fittedDataLast = fitList[len(fitList)-1]
        transformedDataLast = testDataList[len(testDataList)-1]
        featureImportancesFirst = rf_feature_importance(fittedDataFirst,
                                                        transformedDataFirst,
                                                        verbose,
                                                        logger)
        featureImportancesLast = rf_feature_importance(fittedDataLast,
                                                       transformedDataLast,
                                                       verbose,
                                                       logger)

        #prepare feature variables importance for saving
        feature_importances_last = featureImportancesLast.rename(index=str,columns={'name':'variable'})
        feature_importances_last = feature_importances_last.rename(index=str,columns={'feature_importances':
                                                                                          'importance'})
        feature_importances_last['dt_execution'] = first_pred_day
        feature_importances_last['model'] = 'last'
        feature_importances_last['target_variable'] = col_target
        feature_importances_first = featureImportancesFirst.rename(index=str,columns={'name':'variable'})
        feature_importances_first = feature_importances_first.rename(index=str,columns={'feature_importances':
                                                                                            'importance'})
        feature_importances_first['dt_execution'] = first_pred_day
        feature_importances_first['model'] = 'first'
        feature_importances_first['target_variable'] = col_target
        feature_importances_all = feature_importances_last.append(feature_importances_first)
        if verbose:
            logger.info('calc_feat_importance_all step end')
    except Exception:
        logger.exception("Fatal error in calc_feat_importance_all()")
        raise
    return featureImportancesFirst, featureImportancesLast, feature_importances_all