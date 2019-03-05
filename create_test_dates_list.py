# -*- coding: utf-8 -*-
"""
Created in 2018
@author: aszewczyk
Function that creates lists with dates denoting test/prediction sets start and end dates
for each of the consecutive models (ie. model for next 7 days, days 8-14 and so forth)
Input:
    @split_value - datetime object denoting date where the test set starts 
    (ie. tomorrow for model in production) 
Returns:
    @start_days_list - list with dates denoting test set start for each for the consecutive models
    @end_days_list - list with dates denoting test set end for each for the consecutive models
		@verbose - should print logger messages on the screen and save them to .log file?
		@logger - logger connection
"""
from datetime import timedelta
from datetime import datetime
import calendar
def create_test_dates_list(split_value,
                          verbose,
                          logger):
    try:
        if verbose:
            logger.info('Create lists with test/prediction sets start and end dates start, function create_test_dates_list()')
        #change split_value to python datetime format
        split_value = datetime.strptime(split_value, '%Y-%m-%d')
        #find the last evaluation date (last day of next next month after split_value)
        if split_value.month == 8:
            end_next2nd_month_date = datetime(split_value.year+1, 1, calendar.monthrange(split_value.year+1, 1)[1])
        elif split_value.month == 9:
            end_next2nd_month_date = datetime(split_value.year+1, 2, calendar.monthrange(split_value.year+1, 2)[1])
        elif split_value.month == 10:
            end_next2nd_month_date = datetime(split_value.year+1, 3, calendar.monthrange(split_value.year+1, 3)[1])
        elif split_value.month == 11:
            end_next2nd_month_date = datetime(split_value.year+1, 4, calendar.monthrange(split_value.year+1, 4)[1])
        elif split_value.month == 12:
            end_next2nd_month_date = datetime(split_value.year+1, 5, calendar.monthrange(split_value.year+1, 5)[1])
        else:
            end_next2nd_month_date = datetime(split_value.year, split_value.month+5, calendar.monthrange(split_value.year, split_value.month+5)[1])
        #create list with start and end days for each data set
        start_days_list = [(split_value + timedelta(days=0)).strftime("%Y-%m-%d"),
                         (split_value + timedelta(days=8)).strftime("%Y-%m-%d"),
                         (split_value + timedelta(days=15)).strftime("%Y-%m-%d"),
                         (split_value + timedelta(days=33)).strftime("%Y-%m-%d"),
                         (split_value + timedelta(days=46)).strftime("%Y-%m-%d"),
                         (split_value + timedelta(days=64)).strftime("%Y-%m-%d")]

        end_days_list = [(split_value + timedelta(days=7)).strftime("%Y-%m-%d"),
                        (split_value + timedelta(days=14)).strftime("%Y-%m-%d"),
                        (split_value + timedelta(days=32)).strftime("%Y-%m-%d"),
                        (split_value + timedelta(days=45)).strftime("%Y-%m-%d"),
                        (split_value + timedelta(days=63)).strftime("%Y-%m-%d"),
                        (end_next2nd_month_date).strftime("%Y-%m-%d")]
        if verbose:
            logger.info('Create lists with test/prediction sets start and end dates end')
    except Exception:
        logger.exception("Fatal error in create_test_dates_list()")
        raise
    return(start_days_list, end_days_list)
