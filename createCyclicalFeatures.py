"""
Created on Thu May 24 10:36:02 2018
@author: aszewczyk
Function that converts to cyclical variables in the cyclical_variables list. For each variable in the list sin and cos
 of this variable is created as in the following example:
    seconds_in_day = 24*60*60
    df['sin_time'] = np.sin(2*np.pi*df.seconds/seconds_in_day)
    df['cos_time'] = np.cos(2*np.pi*df.seconds/seconds_in_day)
Afterward original variables are removed from data frame.
Input:
    @data - spark df that contains variables to be converted into cyclical ones
    @cyclical_variables - list with strings with cyclical variables column names inside of data d.f.
    @verbose - should print logger messages on the screen and save them to .log file?
	@logger - logger connection
Returns:
    @data - spark df with added cyclical columns
"""

import numpy as np
from pyspark.sql.functions import sin, cos, col
from pyspark.sql.types import DoubleType


def createCyclicalFeatures(
    data,
    cyclical_variables: list,
    drop_orig_vars: bool = False,
    verbose: bool = False,
    logger: bool = False
):
    try:
        if verbose:
            logger.info('create_cyclical_features() start')
        for i in range(len(cyclical_variables)):
            distinct_values_count = data.select(cyclical_variables[i]).distinct().count()
            data = data.withColumn(cyclical_variables[i]+'_sin', sin(2*np.pi*col(cyclical_variables[i]).\
                                                                     cast(DoubleType())/distinct_values_count))
            data = data.withColumn(cyclical_variables[i]+'_cos', cos(2*np.pi*col(cyclical_variables[i]).\
                                                                     cast(DoubleType())/distinct_values_count))
        if drop_orig_vars:
            data = data.drop(*cyclical_variables)
        if verbose:
            logger.info('create_cyclical_features() end')
    except Exception:
        logger.exception("Fatal error in create_cyclical_features()")
        raise

    return data
