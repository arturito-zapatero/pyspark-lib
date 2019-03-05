# -*- coding: utf-8 -*-
"""
Created in 2018
@author: aszewczyk
Function that defines the lists with variables used throughout the demand forecast model script
Input:
    @verbose
    @logger
Returns:
    @named_variables_lists - list with lists with variable names
"""

import string
def define_variable_names(verbose,
                          logger):
    try:
        if verbose:
            logger.info('define_variable_names() start')
        #define outliers' affected columns:
        yield_tickets_cols = [
                             'yield_tickets',
                             'yield_tickets_007',
                             'yield_tickets_014',
                             'yield_tickets_032',
                             'yield_tickets_045',
                             'yield_tickets_063',
                             'yield_tickets_093',
                             'yield_tickets_120',
                             'yield_tickets_last_year',
                             'yield_tickets_typical_last006',
                             'yield_tickets_typical_last013',
                             'yield_tickets_typical_last031',
                             'yield_tickets_typical_last062'
        ]
        #for now doesn't use ancis to get rid of outlier flights
        yield_ancis_cols = [
                             'yield_ancis',
                             'yield_ancis_007',
                             'yield_ancis_014',
                             'yield_ancis_032',
                             'yield_ancis_045',
                             'yield_ancis_063',
                             'yield_ancis_093',
                             'yield_ancis_120',
                             'yield_ancis_last_year',
                             'yield_ancis_typical_last006',
                             'yield_ancis_typical_last013',
                             'yield_ancis_typical_last031',
                             'yield_ancis_typical_last062'
        ]

        lf_cols = [
                    'lf',
                     'lf_007',
                     'lf_014',
                     'lf_032',
                     'lf_045',
                     'lf_063',
                     'lf_093',
                     'lf_120',
                     'lf_last_year',
                     'lf_typical_last006',
                     'lf_typical_last013',
                     'lf_typical_last031',
                     'lf_typical_last062'
        ]

        price_cols = [
                     'price_tickets_typical_last006',
                    'price_tickets_typical_last013',
                    'price_tickets_typical_last031',
                    'price_tickets_typical_last062',
                    'price_tickets_last_year',
                    'avg_price_ticket_007_013',
                    'avg_price_ticket_014_031',
                    'avg_price_ticket_032_044',
                    'avg_price_ticket_045_062',
                    'avg_price_ticket_063_092',
                    'avg_price_ticket_093_119',
                    'avg_price_ticket_before_120'
        ]
        lag_price_cols = [
                             'avg_price_ticket_007_013',
                             'avg_price_ticket_014_031',
                             'avg_price_ticket_032_044',
                             'avg_price_ticket_045_062',
                             'avg_price_ticket_063_092',
                             'avg_price_ticket_093_119',
                             'avg_price_ticket_before_120'
                             ]

        cumul_pax_cols = ['pax_seat', 'cum_pax_seat_007','cum_pax_seat_014','cum_pax_seat_032',
                          'cum_pax_seat_045', 'cum_pax_seat_063', 'cum_pax_seat_093', 'cum_pax_seat_120']
        cumul_lf_cols = ['lf', 'lf_007', 'lf_014', 'lf_032',
                         'lf_045', 'lf_063', 'lf_093', 'lf_120']
        cumul_revenue_ticket_cols = ['revenue_tickets', 'cum_revenue_ticket_007', 'cum_revenue_ticket_014', 'cum_revenue_ticket_032',
                                     'cum_revenue_ticket_045', 'cum_revenue_ticket_063', 'cum_revenue_ticket_093', 'cum_revenue_ticket_120']
        cumul_yield_ticket_cols = ['yield_tickets', 'yield_tickets_007', 'yield_tickets_014', 'yield_tickets_032',
                                   'yield_tickets_045', 'yield_tickets_063', 'yield_tickets_093', 'yield_tickets_120']
        cumul_revenue_ancis_cols = ['ancis', 'cum_revenue_ancis_007', 'cum_revenue_ancis_014', 'cum_revenue_ancis_032',
                                    'cum_revenue_ancis_045', 'cum_revenue_ancis_063', 'cum_revenue_ancis_093', 'cum_revenue_ancis_120']
        cumul_yield_ancis_cols = ['yield_ancis', 'yield_ancis_007', 'yield_ancis_014', 'yield_ancis_032',
                                  'yield_ancis_045', 'yield_ancis_063', 'yield_ancis_093', 'yield_ancis_120']
        cumul_revenue_total_cols = ['revenue_total', 'cum_revenue_total_007', 'cum_revenue_total_014', 'cum_revenue_total_032',
                                     'cum_revenue_total_045', 'cum_revenue_total_063', 'cum_revenue_total_093', 'cum_revenue_total_120']
        cumul_yield_total_cols = ['yield_total', 'yield_total_007', 'yield_total_014', 'yield_total_032',
                                   'yield_total_045', 'yield_total_063', 'yield_total_093', 'yield_total_120']

        last_year_abs_seasonal_cols = ['sum_pax_seat_time_of_day_prev_year',
                                       'sum_revenue_tickets_time_of_day_prev_year',
                                       'sum_ancis_time_of_day_prev_year',
                                       'sum_revenue_total_time_of_day_prev_year'] 

        last_year_rel_seasonal_cols = ['lf_last_year',
                                       'yield_tickets_last_year', 
                                       'yield_ancis_last_year',
                                       'yield_total_last_year']

        sales_last_year_agg1_cols = [                      
                                 'sum_revenue_tickets_time_of_day_prev_year',                        
                                 'sum_ancis_time_of_day_prev_year',                        
                                 'sum_revenue_total_time_of_day_prev_year',                       
                                 'sum_capacity_time_of_day_prev_year',                       
                                 'sum_flights_time_of_day_prev_year',                      
                                 'avg_price_tickets_time_of_day_prev_year',
                                 'sum_pax_seat_time_of_day_prev_year'
                                 ]

        sales_last_year_agg2_cols = [ 
                                 'sum_revenue_tickets_day_of_week_prev_year',
                                 'sum_ancis_day_of_week_prev_year',
                                 'sum_revenue_total_day_of_week_prev_year',
                                 'sum_capacity_day_of_week_prev_year',
                                 'sum_flights_day_of_week_prev_year',
                                 'avg_price_tickets_day_of_week_prev_year',
                                 'sum_pax_seat_day_of_week_prev_year']

        sales_last_year_agg3_cols = [
                                  'sum_revenue_tickets_week_prev_year',
                                  'sum_ancis_week_prev_year',
                                  'sum_revenue_total_week_prev_year',
                                  'sum_capacity_week_prev_year',
                                  'sum_flights_calendar_week_prev_year',
                                  'avg_price_tickets_week_prev_year',
                                  'sum_pax_seat_week_prev_year']

        sales_last_year_agg4_cols = [ 
                                  'sum_revenue_tickets_year_prev_year',
                                  'sum_ancis_year_prev_year',
                                  'sum_revenue_total_year_prev_year',
                                  'sum_capacity_year_prev_year',
                                  'sum_flights_year_prev_year',
                                  'avg_price_tickets_prev_year',
                                  'sum_pax_seat_year_prev_year']
        cumul_prod_view_cols = ['cum_product_view_007', 'cum_product_view_014', 'cum_product_view_032',
        'cum_product_view_045', 'cum_product_view_063', 'cum_product_view_093', 'cum_product_view_120']

        sum_cumul_prod_view_cols = ['sum_cum_product_view_007', 'sum_cum_product_view_014', 'sum_cum_product_view_032',
        'sum_cum_product_view_045', 'sum_cum_product_view_063', 'sum_cum_product_view_093', 'sum_cum_product_view_120']
        cumul_views_per_pax_cols = ['views_per_pax_007', 'views_per_pax_014', 'views_per_pax_032',
        'views_per_pax_045', 'views_per_pax_063', 'views_per_pax_093', 'views_per_pax_120']

        cumul_pax_from_views_cols = ['pax_from_views_007', 'pax_from_views_014', 'pax_from_views_032',
        'pax_from_views_045', 'pax_from_views_063', 'pax_from_views_093', 'pax_from_views_120']

        competition_2h_cols = [
         'min_price_ticket_2h_competition_before_120',
         'max_price_ticket_2h_competition_before_120',
         'avg_price_ticket_2h_competition_before_120',
         'min_price_ticket_2h_competition_093_119',
         'max_price_ticket_2h_competition_093_119',
         'avg_price_ticket_2h_competition_093_119',
         'min_price_ticket_2h_competition_063_092',
         'max_price_ticket_2h_competition_063_092',
         'avg_price_ticket_2h_competition_063_092',
         'min_price_ticket_2h_competition_045_062',
         'max_price_ticket_2h_competition_045_062',
         'avg_price_ticket_2h_competition_045_062',
         'min_price_ticket_2h_competition_033_044',
         'max_price_ticket_2h_competition_033_044',
         'avg_price_ticket_2h_competition_033_044',
         'min_price_ticket_2h_competition_014_032',
         'max_price_ticket_2h_competition_014_032',
         'avg_price_ticket_2h_competition_014_032',
         'min_price_ticket_2h_competition_007_013',
         'max_price_ticket_2h_competition_007_013',
         'avg_price_ticket_2h_competition_007_013'
              ]

        competition_6h_cols = [ 
         'min_price_ticket_6h_competition_before_120',
         'max_price_ticket_6h_competition_before_120',
         'avg_price_ticket_6h_competition_before_120',
         'min_price_ticket_6h_competition_093_119',
         'max_price_ticket_6h_competition_093_119',
         'avg_price_ticket_6h_competition_093_119',
         'min_price_ticket_6h_competition_063_092',
         'max_price_ticket_6h_competition_063_092',
         'avg_price_ticket_6h_competition_063_092',
         'min_price_ticket_6h_competition_045_062',
         'max_price_ticket_6h_competition_045_062',
         'avg_price_ticket_6h_competition_045_062',
         'min_price_ticket_6h_competition_033_044',
         'max_price_ticket_6h_competition_033_044',
         'avg_price_ticket_6h_competition_033_044',
         'min_price_ticket_6h_competition_014_032',
         'max_price_ticket_6h_competition_014_032',
         'avg_price_ticket_6h_competition_014_032',
         'min_price_ticket_6h_competition_007_013',
         'max_price_ticket_6h_competition_007_013',
         'avg_price_ticket_6h_competition_007_013'
             ]

        competition_day_cols = [
         'min_price_ticket_day_competition_before_120',
         'max_price_ticket_day_competition_before_120',
         'avg_price_ticket_day_competition_before_120',
         'min_price_ticket_day_competition_093_119',
         'max_price_ticket_day_competition_093_119',
         'avg_price_ticket_day_competition_093_119',
         'min_price_ticket_day_competition_063_092',
         'max_price_ticket_day_competition_063_092',
         'avg_price_ticket_day_competition_063_092',
         'min_price_ticket_day_competition_045_062',
         'max_price_ticket_day_competition_045_062',
         'avg_price_ticket_day_competition_045_062',
         'min_price_ticket_day_competition_033_044',
         'max_price_ticket_day_competition_033_044',
         'avg_price_ticket_day_competition_033_044',
         'min_price_ticket_day_competition_014_032',
         'max_price_ticket_day_competition_014_032',
         'avg_price_ticket_day_competition_014_032',
         'min_price_ticket_day_competition_007_013',
         'max_price_ticket_day_competition_007_013',
         'avg_price_ticket_day_competition_007_013'
             ]

        #first make the replacement of VY prices that are NANs with min price segment!!!
        prices_vy_cols = [
         'avg_price_ticket_before_120',
         'avg_price_ticket_before_120',
         'avg_price_ticket_before_120',
         'avg_price_ticket_093_119',
         'avg_price_ticket_093_119',
         'avg_price_ticket_093_119',
         'avg_price_ticket_063_092',
         'avg_price_ticket_063_092',
         'avg_price_ticket_063_092',
         'avg_price_ticket_045_062',
         'avg_price_ticket_045_062',
         'avg_price_ticket_045_062',
         'avg_price_ticket_032_044',
         'avg_price_ticket_032_044',
         'avg_price_ticket_032_044',
         'avg_price_ticket_014_031',
         'avg_price_ticket_014_031',
         'avg_price_ticket_014_031',
         'avg_price_ticket_007_013',
         'avg_price_ticket_007_013',
         'avg_price_ticket_007_013'
         ]
        last_days_airport_pair_abs_cols = [ 'pax_sold_airport_pair_last006',
                                         'revenue_tickets_airport_pair_last006',
                                         'ancis_airport_pair_last006',
                                         'revenue_total_airport_pair_last006',
                                         'pax_sold_airport_pair_last013',
                                         'revenue_tickets_airport_pair_last013',
                                         'ancis_airport_pair_last013',
                                         'revenue_total_airport_pair_last013',
                                         'pax_sold_airport_pair_last031',
                                         'revenue_tickets_airport_pair_last031',
                                         'ancis_airport_pair_last031',
                                         'revenue_total_airport_pair_last031',
                                         'pax_sold_airport_pair_last062',
                                         'revenue_tickets_airport_pair_last062',
                                         'ancis_airport_pair_last062',
                                         'revenue_total_airport_pair_last062'
                                          ]
        tmp_list = [string.replace(name, 'pax_sold_airport_pair_', 'lf_airport_pair_') for name in last_days_airport_pair_abs_cols]
        tmp_list = [string.replace(name, 'revenue_tickets_airport_pair_', 'yield_tickets_airport_pair_') for name in tmp_list]
        tmp_list = [string.replace(name, 'ancis_airport_pair_', 'yield_ancis_airport_pair_') for name in tmp_list]
        #below list replaces last_days_airport_pair_cols
        last_days_airport_pair_rel_cols = [string.replace(name, 'revenue_total_airport_pair_', 'yield_total_airport_pair_') for name in tmp_list]

        last_days_seasonal_abs_cols = [
                                    'pax_sold_seasonal_last006',
                                    'revenue_tickets_seasonal_last006',
                                    'ancis_seasonal_last006',
                                    'revenue_total_seasonal_last006',
                                    'pax_sold_seasonal_last013',
                                    'revenue_tickets_seasonal_last013',
                                    'ancis_seasonal_last013',
                                    'revenue_total_seasonal_last013',
                                    'pax_sold_seasonal_last031',
                                    'revenue_tickets_seasonal_last031',
                                    'ancis_seasonal_last031',
                                    'revenue_total_seasonal_last031',
                                    'pax_sold_seasonal_last062',
                                    'revenue_tickets_seasonal_last062',
                                    'ancis_seasonal_last062',
                                    'revenue_total_seasonal_last062'
                                    ]

        tmp_list = [string.replace(name, 'pax_sold_seasonal_', 'lf_typical_') for name in last_days_seasonal_abs_cols]
        tmp_list = [string.replace(name, 'revenue_tickets_seasonal_', 'yield_tickets_typical_') for name in tmp_list]
        tmp_list = [string.replace(name, 'ancis_seasonal_', 'yield_ancis_typical_') for name in tmp_list]
        #below list replaces last_days_seasonal_cols
        last_days_seasonal_rel_cols = [string.replace(name, 'revenue_total_seasonal_', 'yield_total_typical_') for name in tmp_list]
        list_with_all_variables = [yield_tickets_cols, yield_ancis_cols, lf_cols, price_cols,
                       #last_days_seasonal_cols, last_days_airport_pair_cols,
                      lag_price_cols, cumul_pax_cols, cumul_lf_cols, cumul_revenue_ticket_cols, cumul_yield_ticket_cols,
                      cumul_revenue_ancis_cols, cumul_yield_ancis_cols, cumul_revenue_total_cols, cumul_yield_total_cols,
                      last_year_abs_seasonal_cols, last_year_rel_seasonal_cols,
                      sales_last_year_agg1_cols, sales_last_year_agg2_cols, sales_last_year_agg3_cols, sales_last_year_agg4_cols,
                      cumul_prod_view_cols, sum_cumul_prod_view_cols, cumul_views_per_pax_cols, cumul_pax_from_views_cols,
                      competition_2h_cols, competition_6h_cols, competition_day_cols, prices_vy_cols, 
                      last_days_airport_pair_abs_cols, last_days_seasonal_abs_cols, 
                      last_days_airport_pair_rel_cols, last_days_seasonal_rel_cols
                      ]
        names_of_variables_lists = ['yield_tickets_cols', 'yield_ancis_cols', 'lf_cols', 'price_cols',
                      #'last_days_seasonal_cols', 'last_days_airport_pair_cols',
                      'lag_price_cols', 'cumul_pax_cols', 'cumul_lf_cols', 'cumul_revenue_ticket_cols', 'cumul_yield_ticket_cols',
                      'cumul_revenue_ancis_cols', 'cumul_yield_ancis_cols', 'cumul_revenue_total_cols', 'cumul_yield_total_cols',
                      'last_year_abs_seasonal_cols', 'last_year_rel_seasonal_cols',            
                      'sales_last_year_agg1_cols', 'sales_last_year_agg2_cols', 'sales_last_year_agg3_cols', 'sales_last_year_agg4_cols', 
                      'cumul_prod_view_cols', 'sum_cumul_prod_view_cols', 'cumul_views_per_pax_cols', 'cumul_pax_from_views_cols',
                      'competition_2h_cols', 'competition_6h_cols', 'competition_day_cols', 'prices_vy_cols',
                      'last_days_airport_pair_abs_cols', 'last_days_seasonal_abs_cols',
                      'last_days_airport_pair_rel_cols', 'last_days_seasonal_rel_cols']
        named_variables_lists = dict(zip(names_of_variables_lists, list_with_all_variables))
        if verbose:
            logger.info('define_variable_names() start')
    except Exception:
        logger.exception("Fatal error in define_variable_names()")
        raise
    return(named_variables_lists)