# -*- coding: utf-8 -*-
"""
Created in 2018
@author: aszewczyk
Function that creates spark session using beer nomenclature :)
Input:
    @jarra - part of cluster resources used using beer units (e.g. tercio process will use ca. 33% of cluster resources)
    @verbose - should print logger messages on the screen and save them to .log file?
	@logger - logger connection
    #@memoryOverhead = '2g' #by default 0.07 of ParamExecutorMemory
Returns:
    @spark - spark session
"""
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


def createSparkSession(
    jarra: str = 'quinto',
    verbose: bool = False,
    logger: bool = False
):
    try:

        if 'spark' in locals():
            spark.stop()
        if jarra == 'mediana':
            ParamMaxExecutors = '6'
            ParamExecutorMemory = '8g'
        if jarra == 'tercio':
            ParamMaxExecutors = '4'
            ParamExecutorMemory = '8g'
        if jarra == 'quinto':
            ParamMaxExecutors = '3'
            ParamExecutorMemory = '6g'
        if jarra == 'mass':
            ParamMaxExecutors = '12'
            ParamExecutorMemory = '8g'

        if verbose:
            logger.info('Spark process start, jarra: ' + jarra)
        spark = SparkSession\
        .builder\
        .appName("proceso-mediana")\
        .config("spark.dynamicAllocation.enabled", 'true')\
        .config("spark.dynamicAllocation.maxExecutors", ParamMaxExecutors)\
        .config("spark.dynamicAllocation.minExecutors", '1')\
        .config("spark.dynamicAllocation.initialExecutors", '1')\
        .config('spark.executor.cores', '8')\
        .config('spark.cores.max', '12')\
        .config("spark.executor.memory", ParamExecutorMemory)\
        .config("spark.driver.memory", '15g')\
        .config("spark.driver.maxResultSize", '7g')\
        .config("spark.sql.crossJoin.enabled", "true")\
        .config("spark.shuffle.service.enabled", "true")\
        .config("spark.sql.parquet.writeLegacyFormat", "true")\
        .config("spark.executor.memoryOverhead", "2g")\
        .enableHiveSupport()\
        .getOrCreate()
        

        #spark.driver.maxResultSizeLimit of total size of serialized results of all partitions for each Spark action (e.g. collect) in bytes.

        #Should be at least 1M, or 0 for unlimited. Jobs will be aborted if the total size is above this limit. Having a high limit may
        #cause out-of-memory errors in driver (depends on spark.driver.memory and memory overhead of objects in JVM). 
        #No. If estimated size of the data is larger than maxResultSize given job will be aborted. 

        #The goal here is to protect your application from driver loss, nothing more.
        #Setting a proper limit can protect the driver from out-of-memory errors.
        #config('spark.executor.cores', '8')\ #controls how many cores each executor has
        #config("spark.executor.memory", ParamExecutorMemory) need to be changed to enable more Heap space for JVM
        #config('spark.task.cpus', '4')\ # controls how many CPUs each task can use 4 -very long

        sqlContext = SQLContext(spark)  
        spark.sparkContext.setLogLevel("ERROR")
        if verbose:
            logger.info('spark process and sqlContext created succesfully')
    except Exception:
        logger.exception("Fatal error in create_spark_session()")
        raise

    return spark
