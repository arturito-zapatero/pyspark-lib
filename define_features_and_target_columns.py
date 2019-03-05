# -*- coding: utf-8 -*-
"""
Created on Thu May 24 10:36:02 2018
@author: aszewczyk
Defines the feature columns for each of the models, also categorical features for OHE
Defines the target column
Pax and revenue models use the same prediction columns, whereas ancis model uses different ones
Input:
    @model_complex - model complexity, more complex model will use more features
    @use_clustered_data_sets - if model is trained and tested/predicted using clusters or whole data set
    @col_target - string with target column
    @verbose - should print logger messages on the screen and save them to .log file?
		@logger - logger connection
Returns:
    @features_list - list of lists with feature columns for each of the models (with complexity=model_complex),
    excluding the ohe columns
    @features_list_basic_model - list of lists with basic feature columns for each of the models
    (with basic compexity) excluding the ohe columns.
    This basic model is used to predict the data for flights that contain Nulls in some rows
    (e.g. have no historical data)
    @ohe_variables_in - list with categorical columns' names that are transformed in OHE process
"""

def define_features(model_complex='fourth',
                    use_clustered_data_sets=False,
                    col_target='pax_seat',
                    verbose=False,
                    logger=False):
    try:
        if verbose:
            logger.info('Definition of features and target columns start, function define_features_and_target_columns()')
        #OHE variables shuld be the same for basic and other chosen model
        #define one hot encoding variables
        cyclical_variables = [ 'dt_flight_month', 'dt_flight_hour_local_zone', 'dt_flight_iso_calendar_week']
        if use_clustered_data_sets:
            ohe_variables_in = ['cd_airport_pair_reduced', 'id_season', 'cd_time_of_day', 'dt_flight_day_of_week', 'ds_region_cluster_order']
        else:
            #ohe_variables_in = ['cd_airport_pair_reduced', 'id_season', 'cd_time_of_day', 'dt_flight_day_of_week', 'cluster', 'cluster_afp', 'ds_region_cluster_order']
            ohe_variables_in = ['id_season', 'cd_time_of_day', 'dt_flight_day_of_week', 'cluster']
        #define features for models for pax or revenue
        if not col_target=='ancis':
            #basic models
            cols_features_basic = [
             u'seats', u'km', u'airport_pair_frequency_day',
             u'lf_093', u'yield_tickets_093', u'avg_price_ticket_093_119', 
             u'lf_120', u'yield_tickets_120', u'avg_price_ticket_before_120' 
            ]
            cols_features_63d = cols_features_basic + [u'lf_063', u'yield_tickets_063', u'avg_price_ticket_063_092']
            cols_features_45d = cols_features_63d   + [u'lf_045', u'yield_tickets_045', u'avg_price_ticket_045_062']
            cols_features_32d = cols_features_45d   + [u'lf_032', u'yield_tickets_032', u'avg_price_ticket_032_044']
            cols_features_14d = cols_features_32d   + [u'lf_014', u'yield_tickets_014', u'avg_price_ticket_014_031']
            cols_features_7d  = cols_features_14d   + [u'lf_007', u'yield_tickets_007', u'avg_price_ticket_007_013'] 
            #create list with lists with features for each of the consecutive models
            features_list_basic_model = [cols_features_7d, cols_features_14d, cols_features_32d, cols_features_45d, cols_features_63d, cols_features_basic]
            #full models
            #forth level of model complexity adds additionally market share variables
            if model_complex=='fourth':   
                cols_features_basic = [
                 #'is_event_orig', 'is_event_dest', 
                 'last_days_similarity', 'last_year_similarity',
                 'seats', 'km', 'is_charter', 'airport_pair_frequency_day', 'airport_pair_frequency_year', 
                 'lf_093', u'yield_tickets_093', u'avg_price_ticket_093_119', 
                 'lf_120', u'yield_tickets_120', u'avg_price_ticket_before_120',
                 'price_tickets_last_year', 'lf_last_year', 'yield_tickets_last_year',
                 'price_tickets_typical_last006', 'price_tickets_typical_last013', 'price_tickets_typical_last031', 'price_tickets_typical_last062',
                 'lf_typical_last006', 'yield_tickets_typical_last006',
                 'lf_typical_last013', 'yield_tickets_typical_last013',
                 'lf_typical_last031', 'yield_tickets_typical_last031',
                 'lf_typical_last062', 'yield_tickets_typical_last062',
                 'pax_from_views_093', 'pax_from_views_120',
                 'min_price_ticket_2h_competition_before_120', 'max_price_ticket_2h_competition_before_120', 'avg_price_ticket_2h_competition_before_120',
                 'min_price_ticket_2h_competition_093_119', 'max_price_ticket_2h_competition_093_119', 'avg_price_ticket_2h_competition_093_119',
                 'competition_similarity',
                 'vy_2h_market_share', 'vy_2h_lowcost_market_share', 'vy_2h_seats_market_share', 'vy_2h_lowcost_seats_market_share'
                ] 
                cols_features_63d = cols_features_basic + [u'lf_063', u'yield_tickets_063', u'avg_price_ticket_063_092', 'pax_from_views_063', 
                                                           'min_price_ticket_2h_competition_063_092', 'max_price_ticket_2h_competition_063_092', 'avg_price_ticket_2h_competition_063_092']
                cols_features_45d = cols_features_63d   + [u'lf_045', u'yield_tickets_045', u'avg_price_ticket_045_062', 'pax_from_views_045', 
                                                           'min_price_ticket_2h_competition_045_062', 'max_price_ticket_2h_competition_045_062', 'avg_price_ticket_2h_competition_045_062']
                cols_features_32d = cols_features_45d   + [u'lf_032', u'yield_tickets_032', u'avg_price_ticket_032_044', 'pax_from_views_032',  
                                                           'min_price_ticket_2h_competition_033_044', 'max_price_ticket_2h_competition_033_044', 'avg_price_ticket_2h_competition_033_044']
                cols_features_14d = cols_features_32d   + [u'lf_014', u'yield_tickets_014', u'avg_price_ticket_014_031', 'pax_from_views_014',   
                                                           'min_price_ticket_2h_competition_014_032', 'max_price_ticket_2h_competition_014_032', 'avg_price_ticket_2h_competition_014_032']
                cols_features_7d  = cols_features_14d   + [u'lf_007', u'yield_tickets_007', u'avg_price_ticket_007_013', 'pax_from_views_007',  
                                                           'min_price_ticket_2h_competition_007_013', 'max_price_ticket_2h_competition_007_013', 'avg_price_ticket_2h_competition_007_013']
                #create list with lists with features for each of the consecutive models
                features_list = [cols_features_7d, cols_features_14d, cols_features_32d, cols_features_45d, cols_features_63d, cols_features_basic]
            elif model_complex=='basic':
                ohe_variables_in = []
                features_list = features_list_basic_model
        #define features for ancillaries predictions
        if col_target=='ancis':   
            #basic models
            cols_features_basic = [
             u'seats', u'km', u'airport_pair_frequency_day',
             u'lf_093', u'yield_ancis_093', u'avg_price_ticket_093_119', 
             u'lf_120', u'yield_ancis_120', u'avg_price_ticket_before_120' 
            ]
            cols_features_63d = cols_features_basic + [u'lf_063', u'yield_ancis_063', u'avg_price_ticket_063_092']
            cols_features_45d = cols_features_63d   + [u'lf_045', u'yield_ancis_045', u'avg_price_ticket_045_062']
            cols_features_32d = cols_features_45d   + [u'lf_032', u'yield_ancis_032', u'avg_price_ticket_032_044']
            cols_features_14d = cols_features_32d   + [u'lf_014', u'yield_ancis_014', u'avg_price_ticket_014_031']
            cols_features_7d  = cols_features_14d   + [u'lf_007', u'yield_ancis_007', u'avg_price_ticket_007_013']
            #create list with lists with features for each of the consecutive models
            features_list_basic_model = [cols_features_7d, cols_features_14d, cols_features_32d, cols_features_45d, cols_features_63d, cols_features_basic]
            #full models
            cols_features_basic = [
             #'is_event_orig', 'is_event_dest',
             'last_days_similarity', 'last_year_similarity',
             'seats', 'km', 'is_charter', 'airport_pair_frequency_day', 'airport_pair_frequency_year', 
             'lf_093', u'yield_ancis_093', u'avg_price_ticket_093_119', 
             'lf_120', u'yield_ancis_120', u'avg_price_ticket_before_120',
             'price_tickets_last_year', 'lf_last_year', 'yield_ancis_last_year',
             'price_tickets_typical_last006', 'price_tickets_typical_last013', 'price_tickets_typical_last031', 'price_tickets_typical_last062',
             'lf_typical_last006', 'yield_ancis_typical_last006',
             'lf_typical_last013', 'yield_ancis_typical_last013',
             'lf_typical_last031', 'yield_ancis_typical_last031',
             'lf_typical_last062', 'yield_ancis_typical_last062',
             'pax_from_views_093', 'pax_from_views_120',
             'min_price_ticket_2h_competition_before_120', 'max_price_ticket_2h_competition_before_120', 'avg_price_ticket_2h_competition_before_120',
             'min_price_ticket_2h_competition_093_119', 'max_price_ticket_2h_competition_093_119', 'avg_price_ticket_2h_competition_093_119',
             'competition_similarity',
             'vy_2h_market_share', 'vy_2h_lowcost_market_share', 'vy_2h_seats_market_share', 'vy_2h_lowcost_seats_market_share'
            ] 
            cols_features_63d = cols_features_basic + [u'lf_063', u'yield_ancis_063', u'avg_price_ticket_063_092', 'pax_from_views_063', 
                                                       'min_price_ticket_2h_competition_063_092', 'max_price_ticket_2h_competition_063_092',                        'avg_price_ticket_2h_competition_063_092']
            cols_features_45d = cols_features_63d   + [u'lf_045', u'yield_ancis_045', u'avg_price_ticket_045_062', 'pax_from_views_045',                                                     
                                                       'min_price_ticket_2h_competition_045_062', 'max_price_ticket_2h_competition_045_062', 'avg_price_ticket_2h_competition_045_062']
            cols_features_32d = cols_features_45d   + [u'lf_032', u'yield_ancis_032', u'avg_price_ticket_032_044', 'pax_from_views_032',                                                    
                                                       'min_price_ticket_2h_competition_033_044', 'max_price_ticket_2h_competition_033_044', 'avg_price_ticket_2h_competition_033_044']
            cols_features_14d = cols_features_32d   + [u'lf_014', u'yield_ancis_014', u'avg_price_ticket_014_031', 'pax_from_views_014',                                                    
                                                       'min_price_ticket_2h_competition_014_032', 'max_price_ticket_2h_competition_014_032', 'avg_price_ticket_2h_competition_014_032']
            cols_features_7d  = cols_features_14d   + [u'lf_007', u'yield_ancis_007', u'avg_price_ticket_007_013', 'pax_from_views_007',                                                    
                                                       'min_price_ticket_2h_competition_007_013', 'max_price_ticket_2h_competition_007_013', 'avg_price_ticket_2h_competition_007_013']
            #create list with lists with features for each of the consecutive models
            features_list = [cols_features_7d, cols_features_14d, cols_features_32d, cols_features_45d, cols_features_63d, cols_features_basic]
    #features_list = [cols_features_7d, cols_features_14d, cols_features_32d, cols_features_45d, cols_features_63d, cols_features_basic]
        if verbose:
            logger.info('Definition of features and target columns end')
    except Exception:
        logger.exception("Fatal error in define_features_and_target_columns()")
        raise
    return(features_list, features_list_basic_model, ohe_variables_in, cyclical_variables)