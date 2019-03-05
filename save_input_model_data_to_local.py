"""
Created in 11.2018
@author: aszewczyk
Function that saves the clean data set used for demand forecast model in local edge node drive.
Input:
    @data - spark data frame with model input data (features + target + id columns)
 	@local_save_path - local path to save the model results
    @verbose - should print logger
	@logger - logger connection
Returns:
    None
"""
from pyspark.sql.functions import lit, to_date
import os
def save_input_model_data_to_local(data,
                                   spark,
                                   first_pred_day,
                                   local_save_path,
                                   verbose,
                                   logger):
    #add date of execution variable
    input_data_to_save = data.withColumn('dt_execution', lit(first_pred_day))
    input_data_to_save = input_data_to_save.withColumn('dt_execution' ,to_date('dt_execution'))
    input_data_to_save.createOrReplaceTempView("input_data")
    input_data_to_save_tmp = spark.sql("""
    select
    id_flight	as	id_flight,
    cd_airport_pair	as	cd_airport_pair	,
    cd_carrier	as	cd_airline_code	,
    id_season	as	id_season	,
    cd_time_of_day	as	cd_time_of_day	,
    dt_flight_day_of_week	as	dt_flight_day_of_week	,
    dt_flight_date_local	as	dt_flight_date_local_zone	,
    cd_num_flight	as	cd_flight_number	,
    dt_flight_year	as	dt_flight_year	,
    dt_flight_iso_year	as	dt_flight_iso_year	,
    dt_flight_iso_calendar_week	as	dt_flight_iso_calendar_week	,
    dt_flight_month	as	dt_flight_month	,
    ds_time_of_day	as	ds_time_of_day	,
    dt_flight_hour_local_zone	as	dt_flight_hour_local_zone	,
    cd_airport_pair_order	as	cd_airport_pair_order	,
    cd_orig	as	cd_airport_dep	,
    cd_dest	as	cd_airport_arr	,
    cluster	as	macro_cluster	,
    cluster_afp	as	micro_cluster	,
    is_event_orig	as	is_event_dep	,
    is_event_dest	as	is_event_arr	,
    seats	as	seats_adjusted	,
    km	as	km	,
    is_charter	as	is_charter	,
    airport_pair_frequency_day	as	airport_pair_frequency_day	,
    airport_pair_frequency_year	as	airport_pair_frequency_year	,
    ds_market_revenue_orig	as	ds_market_revenue_dep	,
    is_eu_orig	as	is_eu_dep	,
    ds_market_revenue_dest	as	ds_market_revenue_arr	,
    is_eu_dest	as	is_eu_arr	,
    cd_region_orig	as	ds_regional_dep	,
    cd_region_dest	as	ds_regional_arr	,
    ds_region_cluster_order	as	ds_regional_cluster_order	,
    revenue_tickets,
    revenue_tickets_no_gestion,
    pax_seat	as	pax_seat	,
    ancis	as	revenue_ancis	,
    revenue_total	as	revenue_total	,
    dt_flight_year_month	as	dt_flight_year_month	,
    cd_airport_pair_reduced	as	cd_airport_pair_reduced	,
    cum_pax_sold	as	cum_pax_sold	,
    cum_revenue	as	cum_revenue_tickets	,
    cum_price_ticket_avg	as	cum_price_ticket_avg	,
    cum_revenue_ancis	as	cum_revenue_ancis	,
    cum_pax_seat_007	as	cum_pax_seat_007	,
    cum_revenue_ticket_007	as	cum_revenue_ticket_007	,
    avg_price_ticket_007_013	as	avg_price_ticket_007_013	,
    cum_pax_seat_007_013	as	cum_pax_seat_007_013	,
    cum_revenue_ancis_007	as	cum_revenue_ancis_007	,
    cum_pax_seat_014	as	cum_pax_seat_014	,
    cum_revenue_ticket_014	as	cum_revenue_ticket_014	,
    avg_price_ticket_014_031	as	avg_price_ticket_014_031	,
    cum_pax_seat_014_031	as	cum_pax_seat_014_031	,
    cum_revenue_ancis_014	as	cum_revenue_ancis_014	,
    cum_pax_seat_032	as	cum_pax_seat_032	,
    cum_revenue_ticket_032	as	cum_revenue_ticket_032	,
    avg_price_ticket_032_044	as	avg_price_ticket_032_044	,
    cum_pax_seat_032_044	as	cum_pax_seat_032_044	,
    cum_revenue_ancis_032	as	cum_revenue_ancis_032	,
    cum_pax_seat_045	as	cum_pax_seat_045	,
    cum_revenue_ticket_045	as	cum_revenue_ticket_045	,
    avg_price_ticket_045_062	as	avg_price_ticket_045_062	,
    cum_pax_seat_045_062	as	cum_pax_seat_045_062	,
    cum_revenue_ancis_045	as	cum_revenue_ancis_045	,
    cum_pax_seat_063	as	cum_pax_seat_063	,
    cum_revenue_ticket_063	as	cum_revenue_ticket_063	,
    avg_price_ticket_063_092	as	avg_price_ticket_063_092	,
    cum_pax_seat_063_092	as	cum_pax_seat_063_092	,
    cum_revenue_ancis_063	as	cum_revenue_ancis_063	,
    cum_pax_seat_093	as	cum_pax_seat_093	,
    cum_revenue_ticket_093	as	cum_revenue_ticket_093	,
    avg_price_ticket_093_119	as	avg_price_ticket_093_119	,
    cum_pax_seat_093_119	as	cum_pax_seat_093_119	,
    cum_revenue_ancis_093	as	cum_revenue_ancis_093	,
    cum_pax_seat_120	as	cum_pax_seat_120	,
    cum_revenue_ticket_120	as	cum_revenue_ticket_120	,
    cum_revenue_ancis_120	as	cum_revenue_ancis_120	,
    avg_price_ticket_before_120	as	avg_price_ticket_before_120	,
    min_price_ticket_segment	as	min_price_ticket	,
    price_tickets_last_year	as	avg_ly_price_tickets	,
    last_year_similarity	as	ly_similarity_index	,
    price_tickets_typical_last006	as	price_tickets_typical_last006	,
    price_tickets_typical_last013	as	price_tickets_typical_last013	,
    price_tickets_typical_last031	as	price_tickets_typical_last031	,
    price_tickets_typical_last062	as	price_tickets_typical_last062	,
    lf_typical_last006	as	lf_typical_last006	,
    yield_tickets_typical_last006	as	yield_tickets_typical_last006	,
    yield_ancis_typical_last006	as	yield_ancis_typical_last006	,
    yield_total_typical_last006	as	yield_total_typical_last006	,
    lf_typical_last013	as	lf_typical_last013	,
    yield_tickets_typical_last013	as	yield_tickets_typical_last013	,
    yield_ancis_typical_last013	as	yield_ancis_typical_last013	,
    yield_total_typical_last013	as	yield_total_typical_last013	,
    lf_typical_last031	as	lf_typical_last031	,
    yield_tickets_typical_last031	as	yield_tickets_typical_last031	,
    yield_ancis_typical_last031	as	yield_ancis_typical_last031	,
    yield_total_typical_last031	as	yield_total_typical_last031	,
    lf_typical_last062	as	lf_typical_last062	,
    yield_tickets_typical_last062	as	yield_tickets_typical_last062	,
    yield_ancis_typical_last062	as	yield_ancis_typical_last062	,
    yield_total_typical_last062	as	yield_total_typical_last062	,
    ndo	as	days_in_advance	,
    product_view	as	flight_looks	,
    bookings_number	as	flight_books	,
    cum_product_view_007	as	cum_flight_looks_007	,
    cum_product_view_014	as	cum_flight_looks_014	,
    cum_product_view_032	as	cum_flight_looks_032	,
    cum_product_view_045	as	cum_flight_looks_045	,
    cum_product_view_063	as	cum_flight_looks_063	,
    cum_product_view_093	as	cum_flight_looks_093	,
    cum_product_view_120	as	cum_flight_looks_120	,
    min_price_ticket_2h_competition_before_120	as	min_price_ticket_2h_competition_before_120	,
    max_price_ticket_2h_competition_before_120	as	max_price_ticket_2h_competition_before_120	,
    avg_price_ticket_2h_competition_before_120	as	avg_price_ticket_2h_competition_before_120	,
    min_price_ticket_2h_competition_093_119	as	min_price_ticket_2h_competition_093_119	,
    max_price_ticket_2h_competition_093_119	as	max_price_ticket_2h_competition_093_119	,
    avg_price_ticket_2h_competition_093_119	as	avg_price_ticket_2h_competition_093_119	,
    min_price_ticket_2h_competition_063_092	as	min_price_ticket_2h_competition_063_092	,
    max_price_ticket_2h_competition_063_092	as	max_price_ticket_2h_competition_063_092	,
    avg_price_ticket_2h_competition_063_092	as	avg_price_ticket_2h_competition_063_092	,
    min_price_ticket_2h_competition_045_062	as	min_price_ticket_2h_competition_045_062	,
    max_price_ticket_2h_competition_045_062	as	max_price_ticket_2h_competition_045_062	,
    avg_price_ticket_2h_competition_045_062	as	avg_price_ticket_2h_competition_045_062	,
    min_price_ticket_2h_competition_033_044	as	min_price_ticket_2h_competition_033_044	,
    max_price_ticket_2h_competition_033_044	as	max_price_ticket_2h_competition_033_044	,
    avg_price_ticket_2h_competition_033_044	as	avg_price_ticket_2h_competition_033_044	,
    min_price_ticket_2h_competition_014_032	as	min_price_ticket_2h_competition_014_032	,
    max_price_ticket_2h_competition_014_032	as	max_price_ticket_2h_competition_014_032	,
    avg_price_ticket_2h_competition_014_032	as	avg_price_ticket_2h_competition_014_032	,
    min_price_ticket_2h_competition_007_013	as	min_price_ticket_2h_competition_007_013	,
    max_price_ticket_2h_competition_007_013	as	max_price_ticket_2h_competition_007_013	,
    avg_price_ticket_2h_competition_007_013	as	avg_price_ticket_2h_competition_007_013	,
    min_price_ticket_2h_competition_000_006	as	min_price_ticket_2h_competition_000_006	,
    max_price_ticket_2h_competition_000_006	as	max_price_ticket_2h_competition_000_006	,
    avg_price_ticket_2h_competition_000_006	as	avg_price_ticket_2h_competition_000_006	,
    min_price_ticket_6h_competition_000_006	as	min_price_ticket_6h_competition_000_006	,
    max_price_ticket_6h_competition_000_006	as	max_price_ticket_6h_competition_000_006	,
    avg_price_ticket_6h_competition_000_006	as	avg_price_ticket_6h_competition_000_006	,
    min_price_ticket_day_competition_000_006	as	min_price_ticket_day_competition_000_006	,
    max_price_ticket_day_competition_000_006	as	max_price_ticket_day_competition_000_006	,
    avg_price_ticket_day_competition_000_006	as	avg_price_ticket_day_competition_000_006	,
    vy_2h_frequency	as	vy_2h_frequency	,
    total_2h_frequency	as	total_2h_frequency	,
    lowcost_2h_frequency	as	lowcost_2h_frequency	,
    vy_2h_market_share	as	vy_2h_market_share	,
    vy_2h_lowcost_market_share	as	vy_2h_lowcost_market_share	,
    vy_2h_seats	as	vy_2h_seats	,
    total_2h_seats	as	total_2h_seats	,
    lowcost_2h_seats	as	lowcost_2h_seats	,
    vy_2h_seats_market_share	as	vy_2h_seats_market_share	,
    vy_2h_lowcost_seats_market_share	as	vy_2h_lowcost_seats_market_share	,
    last_days_similarity	as	typical_similarity_index	,
    lf	as	lf	,
    lf_007	as	lf_007	,
    lf_014	as	lf_014	,
    lf_032	as	lf_032	,
    lf_045	as	lf_045	,
    lf_063	as	lf_063	,
    lf_093	as	lf_093	,
    lf_120	as	lf_120	,
    yield_tickets	as	yield_tickets	,
    yield_tickets_007	as	yield_tickets_007	,
    yield_tickets_014	as	yield_tickets_014	,
    yield_tickets_032	as	yield_tickets_032	,
    yield_tickets_045	as	yield_tickets_045	,
    yield_tickets_063	as	yield_tickets_063	,
    yield_tickets_093	as	yield_tickets_093	,
    yield_tickets_120	as	yield_tickets_120	,
    yield_ancis	as	yield_ancis	,
    yield_ancis_007	as	yield_ancis_007	,
    yield_ancis_014	as	yield_ancis_014	,
    yield_ancis_032	as	yield_ancis_032	,
    yield_ancis_045	as	yield_ancis_045	,
    yield_ancis_063	as	yield_ancis_063	,
    yield_ancis_093	as	yield_ancis_093	,
    yield_ancis_120	as	yield_ancis_120	,
    lf_last_year	as	lf_ly	,
    yield_tickets_last_year	as	yield_tickets_ly	,
    yield_ancis_last_year	as	yield_ancis_ly	,
    yield_total_last_year	as	yield_total_ly	,
    competition_similarity	as	competitor_similarity_index	,
    pax_from_views_007	as	pax_from_looks_007	,
    pax_from_views_014	as	pax_from_looks_014	,
    pax_from_views_032	as	pax_from_looks_032	,
    pax_from_views_045	as	pax_from_looks_045	,
    pax_from_views_063	as	pax_from_looks_063	,
    pax_from_views_093	as	pax_from_looks_093	,
    pax_from_views_120	as	pax_from_looks_120	,
    dt_execution	as	dt_execution
    from input_data
    """)
    	
    if not os.path.isdir(local_save_path):
        os.mkdir(local_save_path)
    input_data_to_save_tmp\
    .toPandas()\
    .to_csv(local_save_path + 'model_data_'+ first_pred_day.replace('-', '_') + '.csv', index=False)
    return()