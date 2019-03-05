# -*- coding: utf-8 -*-
"""
Created in 2018
@author: aszewczyk
Function that joins the pre-tables to obtain full data set for demand forecast model.
Tables are left joined to the flight master table, which is
aggregated on flight level.
Input:
    @ds_flight_master - table with flight descriptions (e.g. distance, cluster, capacity,) and target variables 
    @ds_cumulative_data - cumulative sales data calculated from target variables in given NDO ranges 
    (e.g. cumul. pax sold until NDO 120, avg price between NDOs 7 and 14)
    @ds_sales_last_year - typical sales from similar flights last year (flight is characterised by 
    carrier, airport pair, day of week and time of day)
    (e.g. for VY BCNMAD flight on Monday morning, sales last year are calculated by averaging 
    all VY BCNMAD flights on Monday morning 52 weeks before)
    @ds_sales_last_days_seasonal - typical sales seasonal for similar flights in the last days 
    (flight is characterised by carrier, airport pair,
    day of week, season and time of day)
    (e.g. for VY BCNMAD flight on Monday morning in summer, typical sales during 7 days before
    departure are calculated by averaging all VY BCNMAD flights on Monday morning in summer
    during last 12 months)
    @ds_sales_last_days_airport_pair - as ds_sales_last_days_seasonal but on higher 
    (carrier, airport pair) aggragation level
    @ds_looktobook_cumulative_data - cumulative look to book data
    @ds_competitor_data - data about competitors prices
    @ds_market_share_data - data about VY market share
    @ds_holidays_data - data about holidays
    More information about the data in SQL queries
		@verbose - should print logger messages on the screen and save them to .log file?
		@logger - logger connection
Returns:
    @demand_data - full table with (un-cleaned) data for model aggregated on flight level.
"""
from  pyspark.sql.functions import broadcast
def join_demand_tables(ds_flight_master,
						ds_cumulative_data,
						ds_sales_last_year,
						ds_sales_last_days_seasonal,
						ds_sales_last_days_airport_pair,
						ds_looktobook_cumulative_data,
						ds_competitor_data_2h,
            ds_competitor_data_6h,
            ds_competitor_data_day,
						ds_market_share_data,
						ds_holidays_data,
            verbose,
            logger):
    try:
        if verbose:
            logger.info('Joining data tables, start, function join_demand_tables()')
        #left join to the master table (usually on date and (flight number or segment)). Drop duplicated columns
        demand_data_tmp1 = ds_flight_master.join(ds_cumulative_data.drop('dt_flight_year_month'), ["dt_flight_date_local", "cd_airport_pair", "cd_num_flight", "cd_carrier"], "leftouter")
        demand_data_tmp1 = demand_data_tmp1.drop('days_in_advance')
        demand_data_tmp3 = broadcast(demand_data_tmp1).join(ds_sales_last_year, (ds_flight_master.cd_airport_pair == ds_sales_last_year.cd_airport_pair)
                                               & (ds_flight_master.dt_flight_iso_calendar_week == ds_sales_last_year.dt_flight_iso_calendar_week)
                                               & (ds_flight_master.cd_time_of_day == ds_sales_last_year.cd_time_of_day)
                                               & ((ds_flight_master.dt_flight_iso_year - 1) == ds_sales_last_year.dt_flight_iso_year)
                                               & (ds_flight_master.dt_flight_day_of_week == ds_sales_last_year.dt_flight_day_of_week)
                                               & (ds_flight_master.cd_carrier == ds_sales_last_year.cd_carrier)
                                               , "leftouter")
        demand_data_tmp3 = demand_data_tmp3.drop(ds_sales_last_year.dt_flight_day_of_week)
        demand_data_tmp3 = demand_data_tmp3.drop(ds_sales_last_year.cd_airport_pair)
        demand_data_tmp3 = demand_data_tmp3.drop(ds_sales_last_year.cd_time_of_day)
        demand_data_tmp3 = demand_data_tmp3.drop(ds_sales_last_year.dt_flight_iso_calendar_week)
        demand_data_tmp3 = demand_data_tmp3.drop(ds_sales_last_year.dt_flight_iso_year)
        demand_data_tmp3 = demand_data_tmp3.drop(ds_sales_last_year.cd_carrier)
        demand_data_tmp3 = demand_data_tmp3.drop(ds_sales_last_year.dt_flight_year_month)

        demand_data_tmp4 = (demand_data_tmp3).join((ds_sales_last_days_seasonal),
                                                   ["cd_carrier", "cd_airport_pair", "id_season", "cd_time_of_day", "dt_flight_day_of_week"], "leftouter").cache()
        demand_data_tmp4_1 = demand_data_tmp4.join(ds_sales_last_days_airport_pair, ["cd_carrier", "cd_airport_pair"], "leftouter") 

        demand_data_tmp5 = demand_data_tmp4_1.join(ds_looktobook_cumulative_data, (demand_data_tmp4.cd_airport_pair == ds_looktobook_cumulative_data.cd_airport_pair)
                                               & (demand_data_tmp4.dt_flight_date_local == ds_looktobook_cumulative_data.dt_flight)                                     
                                               , "leftouter").cache()
        demand_data_tmp5 = demand_data_tmp5.drop(ds_looktobook_cumulative_data.dt_flight)
        demand_data_tmp5 = demand_data_tmp5.drop(ds_looktobook_cumulative_data.cd_airport_pair)
        demand_data_tmp6 = demand_data_tmp5.drop(ds_looktobook_cumulative_data.dt_flight_year_month)

        demand_data_tmp7 = demand_data_tmp6.join(ds_competitor_data_2h,
                                                 (demand_data_tmp6.cd_airport_pair == ds_competitor_data_2h.cd_airport_pair)
                                               & (demand_data_tmp6.dt_flight_date_local == ds_competitor_data_2h.dt_flight_date_local)
                                               & (demand_data_tmp6.dt_flight_hour_local_zone == ds_competitor_data_2h.dt_flight_hour)
                                               , "leftouter")
        demand_data_tmp7 = demand_data_tmp7.drop(ds_competitor_data_2h.cd_airport_pair)
        demand_data_tmp7 = demand_data_tmp7.drop(ds_competitor_data_2h.dt_flight_date_local)
        demand_data_tmp7 = demand_data_tmp7.drop(ds_competitor_data_2h.dt_flight_hour)
        demand_data_tmp7 = demand_data_tmp7.drop(ds_competitor_data_2h.is_vy_carrier)
        demand_data_tmp7 = demand_data_tmp7.drop(ds_competitor_data_2h.dt_flight_year_month)

        demand_data_tmp8 = demand_data_tmp7.join(ds_competitor_data_6h,
                                                 (demand_data_tmp7.cd_airport_pair == ds_competitor_data_6h.cd_airport_pair)
                                               & (demand_data_tmp7.dt_flight_date_local == ds_competitor_data_6h.dt_flight_date_local)
                                               & (demand_data_tmp7.dt_flight_hour_local_zone == ds_competitor_data_6h.dt_flight_hour)
                                               , "leftouter")
        demand_data_tmp8 = demand_data_tmp8.drop(ds_competitor_data_6h.cd_airport_pair)
        demand_data_tmp8 = demand_data_tmp8.drop(ds_competitor_data_6h.dt_flight_date_local)
        demand_data_tmp8 = demand_data_tmp8.drop(ds_competitor_data_6h.dt_flight_hour)
        demand_data_tmp8 = demand_data_tmp8.drop(ds_competitor_data_6h.is_vy_carrier)
        demand_data_tmp8 = demand_data_tmp8.drop(ds_competitor_data_6h.dt_flight_year_month)

        demand_data = demand_data_tmp8.join(ds_competitor_data_day,
                                                 (demand_data_tmp8.cd_airport_pair == ds_competitor_data_day.cd_airport_pair)
                                               & (demand_data_tmp8.dt_flight_date_local == ds_competitor_data_day.dt_flight_date_local)
                                               , "leftouter")
        demand_data = demand_data.drop(ds_competitor_data_day.cd_airport_pair)
        demand_data = demand_data.drop(ds_competitor_data_day.dt_flight_date_local)
        demand_data = demand_data.drop(ds_competitor_data_day.is_vy_carrier)
        demand_data = demand_data.drop(ds_competitor_data_day.dt_flight_year_month)

        demand_data = demand_data.join(ds_market_share_data,
                                                 (demand_data.cd_airport_pair == ds_market_share_data.cd_airport_pair)
                                               & (demand_data.dt_flight_date_local == ds_market_share_data.dt_flight_date_local)
                                               & (demand_data.dt_flight_hour_local_zone == ds_market_share_data.dt_flight_hour)
                                               , "leftouter")

        demand_data = demand_data.drop(ds_market_share_data.cd_airport_pair)
        demand_data = demand_data.drop(ds_market_share_data.dt_flight_date_local)
        demand_data = demand_data.drop(ds_market_share_data.dt_flight_hour)
        demand_data = demand_data.drop(ds_market_share_data.dt_flight_year_month)
        if verbose:
            logger.info('Joining data tables, end')
    except Exception:
        logger.exception("Fatal error in join_demand_tables()")
        raise
    return(demand_data)