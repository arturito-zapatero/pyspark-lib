"""
Created on 13.11.2018
@author: aszewczyk
Function that extracts the features importances from pyspark RF model data 
Input:
    @fittedData - pyspark.ml.regression.RandomForestRegressionModel object
    @transformedData - pyspark DF with RF model results 
    @verbose - should print logger messages on the screen and save them to .log file?
	@logger - logger connection
Returns:
    feature_importances - df with features and feat. importances, ordered by feat. importances
"""
import pandas as pd


def RFFeatureImportance(fittedData,
                        transformedData,
                        verbose,
                        logger):
    try:
        if verbose:
            logger.info('rf_feature_importance() start')

        # Gets column names, including OHE columns
        df1 = pd.DataFrame(transformedData\
                           .schema["features"]\
                           .metadata["ml_attr"]["attrs"]["binary"]+
                           transformedData\
                           .schema["features"]\
                           .metadata["ml_attr"]["attrs"]["numeric"])\
                .sort_values("idx")

        # Get model feature importances
        df2 = pd.DataFrame(fittedData.featureImportances.toArray())

        # Join and order
        df1.reset_index(drop=True, inplace=True)
        df2.reset_index(drop=True, inplace=True)
        featureImportances = pd.concat([df1, df2], axis=1).drop('idx', axis=1)
        featureImportances = featureImportances.rename(columns={0:"feature_importances"})
        featureImportances = featureImportances.sort_values('feature_importances', ascending=False)
        if verbose:
            logger.info('rf_feature_importance() end')
    except Exception:
        logger.exception("Fatal error in rf_feature_importance()")
        raise
    return featureImportances