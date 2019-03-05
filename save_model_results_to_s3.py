"""
Created in 11.2018
@author: aszewczyk
Function that loads, joins and saves in local and copies to S3 the pax, revenue ticket 
and revenue ancis predictions. This predictions are created using main predcition script
(predict_demand_main()) and saved in edge node's local directory as .csv files.
In S3 results are also saved as .csv files. This script should be run from the directory
where the model predictions are saved by predict_demand_main().
Input:
    @first_pred_day - first day for prediction (ie. tomorrow), should be the same as used for predictions and as in dt?execution column
    (ie. in function predict_demand_main()), format '2017-01-01'
    @key - AWS user key
    @skey - AWS user secret key
 	@verbose - should print logger messages on the screen and save them to .log file?
	@logger - logger connection
Returns:
    None
Example:
    save_model_results_to_s3(first_pred_day='2018-11-05',key = 'AKIAJZP7W27ES7VPM7GA',
                             skey = 'nOcT4HwvOZJZ9LyuRVYlrdxSxWdJAuAlEWZGOLKo', verbose=True, logger=False)
"""
import pandas as pd
import os
import s3fs
from datetime import datetime
from ConfigParser import SafeConfigParser
def save_model_results_to_s3(first_pred_day=False,
                             key = 'AKIAJZP7W27ES7VPM7GA',
                             skey = 'nOcT4HwvOZJZ9LyuRVYlrdxSxWdJAuAlEWZGOLKo',
                             logger=False,
                             verbose=False):
    if verbose and logger:
        logger.info('save_model_results_to_s3: start')
    # config .ini file has to be in the same directory as df script
    local_path = os.getcwd() + '/'
    #load params from config file
    parser = SafeConfigParser()
    config_file_path = local_path + 'config/demand_forecast_config.conf' #set as S3 path
    parser.read(config_file_path)
    if first_pred_day:
        split_value = first_pred_day
        split_value = datetime.strptime(split_value, '%Y-%m-%d')
    else:
        split_value = datetime.today()
    s3_feat_import_save_path = parser.get('save_params', 's3_feat_import_save_path')
    s3_save_path_full = s3_feat_import_save_path + 'dt-execution=' + split_value.strftime("%Y-%m-%d") + '/'
    #load model results data for each target variable
    if verbose:
        print('Saving demand forecast predictions in: ' + local_path+'data/results/')
        if logger:
            logger.info('Saving demand forecast predictions in: ' + local_path)
    pax_seat_results = pd.read_csv(local_path + 'data/results/' + 'pax_seat_results_'+first_pred_day.replace('-', '_')+'.csv').loc[:,[u'dt_flight_date_local', u'cd_airport_pair', u'cd_airport_pair_order',
            u'cd_num_flight', u'pax_seat_pred']]
    revenue_tickets_results = pd.read_csv(local_path + 'data/results/' + 'revenue_tickets_results_'+first_pred_day.replace('-', '_')+'.csv').loc[:,[u'dt_flight_date_local', u'cd_airport_pair', u'cd_airport_pair_order',
            u'cd_num_flight', u'revenue_tickets_pred']]
    ancis_results = pd.read_csv(local_path + 'data/results/' + 'ancis_results_'+first_pred_day.replace('-', '_')+'.csv').loc[:,[u'dt_flight_date_local', u'cd_airport_pair', u'cd_airport_pair_order',
            u'cd_num_flight', u'ancis_pred']]
    full_demand_forecast_results = pax_seat_results\
                                  .merge(revenue_tickets_results, how='left', on=['dt_flight_date_local', 'cd_airport_pair', 'cd_airport_pair_order', u'cd_num_flight'])\
                                  .merge(ancis_results, how='left', on=['dt_flight_date_local', 'cd_airport_pair', 'cd_airport_pair_order', u'cd_num_flight'])
    #rename some columns to comply with the Nomenclature
    full_demand_forecast_results = full_demand_forecast_results.\
                                   rename(columns={
                                  'dt_flight_date_local':'dt_flight_date_local_zone',
                                  'pax_seat_pred':'target_pax_seat',
                                  'revenue_tickets_pred':'target_revenue_tickets',
                                  'ancis_pred':'target_ancis',                
                                  'cd_num_flight':'cd_flight_number',                
                                                  }, inplace=False)
    #ad-hoc for NaNs in ancis - replace ancis value with *% of tickets revenue!
    full_demand_forecast_results.target_ancis = full_demand_forecast_results.target_ancis.fillna(full_demand_forecast_results.target_revenue_tickets*0.09)
    #save to local
    full_demand_forecast_results.dt_flight_date_local_zone = pd.to_datetime(full_demand_forecast_results.dt_flight_date_local_zone)
    full_demand_forecast_results\
    .to_csv(local_path + 'data/results/' + 'full_results.csv', header=False, sep='|', index=False)

    if verbose:
        print('Copying demand forecast predictions to: ' + str(s3_save_path_full))
        if logger:
            logger.info('Copying demand forecast predictions to: ' + str(s3_save_path_full))
    s3_fs = s3fs.S3FileSystem(key=key, secret=skey)
    s3_fs.put(local_path + 'data/results/' + "full_results.csv", 
        s3_save_path_full+"full_results.csv")

    if verbose:
        print('save_model_results_to_s3: end')
        if logger:
            logger.info('save_model_results_to_s3: end')
    return()