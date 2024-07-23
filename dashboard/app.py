import os
import duckdb
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from duckdb import DuckDBPyConnection
from pandas import DataFrame

################## Utils Functions ######################

st.cache_resource()
def duckdbConn() -> DuckDBPyConnection:
    duckdbPath = os.environ['DUCKDB_PATH']
    return duckdb.connect(f"{duckdbPath}/duckdb.db", read_only=True)

conn = duckdbConn()

st.cache_data(ttl=3600)
def getFilterData(option:str) -> list:

    if option == 'year':
        op = conn.query('select distinct year from silver.dim_calendar').df()['year'].tolist()
    elif option == 'service':
        op = conn.query('select distinct service_desc from silver.dim_services').df()['service_desc'].tolist()
    else:
        op = conn.query('select distinct vendor_desc from silver.dim_vendors').df()['vendor_desc'].tolist()

    return op

st.cache_data(ttl=3600)
def getData(query:str) -> DataFrame:
    return conn.query(query).df()


################## Initial Settings ######################


config = dict({"displayModeBar":'hover',"scrollZoom":False,"displaylogo":False,"responsive":False,"autosizable":True})

st.set_page_config(
    page_title='NYC TLC Trip Data',
    page_icon=':taxi:',
    layout='wide'
)

st.markdown("<h1 style='text-align: center;'> <img src='https://cdn-icons-png.flaticon.com/512/5900/5900437.png' width='50' height='50'> New York Taxi Trips</h1>", unsafe_allow_html=True)
st.divider()

################## Filers ######################


with st.sidebar:

    st.markdown("<h1 style='text-align: center;'>Filters</h1>", unsafe_allow_html=True)

    years = getFilterData('year')
    services = getFilterData('service')
    vendors = getFilterData('vendor')

    serviceOption = st.multiselect('Service', services)
    if len(serviceOption) == 0:
        serviceOption = services

    vendorOption = st.multiselect('Vendor', vendors)
    if len(vendorOption) == 0:
        vendorOption = vendors

    yearOption = st.slider('Year', min(years), max(years), (min(years), max(years)))

################## Querys ######################

totals = getData(f'''select
                            COUNT(*) AS total_trips
                            , SUM(total_amount) AS total_amount
                            , SUM(passenger_count) AS total_passenger
                            , AVG(duration_trip) AS avg_trip_duration
                            , AVG(fare_amount) AS avg_fare
                            , AVG(passenger_count) AS avg_total_passenger
                            , AVG(trip_distance_km) AS avg_trip_distance
                        from silver.fat_trips a
                        inner join silver.dim_calendar b on a.pickup_datetime::date = b.date
                        inner join silver.dim_services c on a.service_key = c.service_key
                        inner join silver.dim_vendors  d on a.vendor_key = d.vendor_key
                        where (b.year BETWEEN ({yearOption[0]}) and {yearOption[1]}) and c.service_desc IN ({', '.join(repr(e) for e in serviceOption)}) and d.vendor_desc IN ({', '.join(repr(e) for e in vendorOption)})''')

total_trips_by_yearmonth = getData(f'''select
                                            count(*) as total_trips
                                            , concat(b.year, '.', b.month_name) as year_month
                                            , b.month
                                            , b.year
                                        from silver.fat_trips a
                                        inner join silver.dim_calendar b on a.pickup_datetime::date = b.date
                                        inner join silver.dim_services c on a.service_key = c.service_key
                                        inner join silver.dim_vendors  d on a.vendor_key = d.vendor_key
                                        where (b.year BETWEEN ({yearOption[0]}) and {yearOption[1]}) and c.service_desc IN ({', '.join(repr(e) for e in serviceOption)}) and d.vendor_desc IN ({', '.join(repr(e) for e in vendorOption)})
                                        group by b.month, b.year, year_month
                                        order by b.year, b.month''')

total_trips_by_weak = getData(f'''select
                                        count(*) as total_trips
                                        , b.day_of_week_name
                                        , b.day_of_week
                                    from silver.fat_trips a
                                    inner join silver.dim_calendar b on a.pickup_datetime::date = b.date
                                    inner join silver.dim_services c on a.service_key = c.service_key
                                    inner join silver.dim_vendors  d on a.vendor_key = d.vendor_key
                                    where (b.year BETWEEN ({yearOption[0]}) and {yearOption[1]}) and c.service_desc IN ({', '.join(repr(e) for e in serviceOption)}) and d.vendor_desc IN ({', '.join(repr(e) for e in vendorOption)})
                                    group by b.day_of_week_name, b.day_of_week
                                    order by 3''')

total_trips_by_vendor = getData(f'''select
                                            count(*) as total_trips
                                            , vendor_desc
                                        from silver.fat_trips a
                                        left join silver.dim_vendors   b on a.vendor_key = b.vendor_key
                                        inner join silver.dim_calendar c on a.pickup_datetime::date = c.date
                                        inner join silver.dim_services d on a.service_key = d.service_key
                                        where (c.year BETWEEN ({yearOption[0]}) and {yearOption[1]}) and d.service_desc IN ({', '.join(repr(e) for e in serviceOption)}) and b.vendor_desc IN ({', '.join(repr(e) for e in vendorOption)})
                                        group by vendor_desc;''')

total_extra_by_services = getData(f'''select
                                            sum(extra) as total_extra
                                            , service_desc
                                        from silver.fat_trips a
                                        left join silver.dim_services  b on a.service_key = b.service_key
                                        inner join silver.dim_calendar c on a.pickup_datetime::date = c.date
                                        inner join silver.dim_vendors  d on a.vendor_key = d.vendor_key
                                        where (c.year BETWEEN ({yearOption[0]}) and {yearOption[1]}) and b.service_desc IN ({', '.join(repr(e) for e in serviceOption)}) and d.vendor_desc IN ({', '.join(repr(e) for e in vendorOption)})
                                        group by service_desc;''')

avg_trip_duration = getData(f'''select
                                        round(avg(duration_trip),2) as avg_trip_duration_min
                                        , b.day_of_week_name
                                        , b.day_of_week
                                    from silver.fat_trips a
                                    inner join silver.dim_calendar b on a.pickup_datetime::date = b.date
                                    inner join silver.dim_services c on a.service_key = c.service_key
                                    inner join silver.dim_vendors  d on a.vendor_key = d.vendor_key
                                    where (b.year BETWEEN ({yearOption[0]}) and {yearOption[1]}) and c.service_desc IN ({', '.join(repr(e) for e in serviceOption)}) and d.vendor_desc IN ({', '.join(repr(e) for e in vendorOption)})
                                    group by b.day_of_week_name, b.day_of_week
                                    order by 3''')

avg_trip_distance = getData(f'''select
                                        round(avg(trip_distance_km),2) as avg_trip_distance_km
                                        , b.quarter
                                        , b.year
                                    from silver.fat_trips a
                                    inner join silver.dim_calendar b on a.pickup_datetime::date = b.date
                                    inner join silver.dim_services c on a.service_key = c.service_key
                                    inner join silver.dim_vendors  d on a.vendor_key = d.vendor_key
                                    where (b.year BETWEEN ({yearOption[0]}) and {yearOption[1]}) and c.service_desc IN ({', '.join(repr(e) for e in serviceOption)}) and d.vendor_desc IN ({', '.join(repr(e) for e in vendorOption)})
                                    group by b.year, b.quarter
                                    order by 3 ''')

trips_by_payments_type = getData(f'''select
                                        count(*) as total_trips
                                        , payment_desc
                                    from silver.fat_trips a
                                    left join silver.dim_payments b on a.payment_key = b.payment_key
                                    inner join silver.dim_calendar c on a.pickup_datetime::date = c.date
                                    inner join silver.dim_services d on a.service_key = d.service_key
                                    inner join silver.dim_vendors  e on a.vendor_key = e.vendor_key
                                    where (c.year BETWEEN ({yearOption[0]}) and {yearOption[1]}) and d.service_desc IN ({', '.join(repr(e) for e in serviceOption)}) and e.vendor_desc IN ({', '.join(repr(e) for e in vendorOption)})
                                    group by payment_desc''')

amount_by_ratecodes = getData(f'''select
                                        avg(total_amount) as avg_total_amount
                                        , ratecode_desc
                                    from silver.fat_trips a
                                    left join silver.dim_ratecodes b on a.ratecode_key = b.ratecode_key
                                    inner join silver.dim_calendar c on a.pickup_datetime::date = c.date
                                    inner join silver.dim_services d on a.service_key = d.service_key
                                    inner join silver.dim_vendors  e on a.vendor_key = e.vendor_key
                                    where (c.year BETWEEN ({yearOption[0]}) and {yearOption[1]}) and d.service_desc IN ({', '.join(repr(e) for e in serviceOption)}) and e.vendor_desc IN ({', '.join(repr(e) for e in vendorOption)})
                                    group by ratecode_desc;''')

amout_by_year = getData(f'''select
                                    sum(total_amount) as total_amount
                                    , c.year
                                from silver.fat_trips a
                                inner join silver.dim_services b on a.service_key = b.service_key
                                inner join silver.dim_calendar c on a.pickup_datetime::date = c.date
                                inner join silver.dim_vendors  d on a.vendor_key = d.vendor_key
                                where (c.year BETWEEN ({yearOption[0]}) and {yearOption[1]}) and b.service_desc IN ({', '.join(repr(e) for e in serviceOption)}) and d.vendor_desc IN ({', '.join(repr(e) for e in vendorOption)})
                                group by c.year
                                order by c.year;''')

trips_by_zones_pu = getData(f'''select
                                        count(*) as total_trips
                                        , b.zone as pickup_zone
                                from silver.fat_trips a
                                left join silver.dim_pu_zones b on a.pu_zone_key = b.pu_zone_key
                                inner join silver.dim_calendar c on a.pickup_datetime::date = c.date
                                inner join silver.dim_services d on a.service_key = d.service_key
                                inner join silver.dim_vendors  e on a.vendor_key = e.vendor_key
                                where (c.year BETWEEN ({yearOption[0]}) and {yearOption[1]}) and d.service_desc IN ({', '.join(repr(e) for e in serviceOption)}) and e.vendor_desc IN ({', '.join(repr(e) for e in vendorOption)})
                                group by b.zone
                                order by 1 desc
                                limit 5''')

trips_by_zones_do = getData(f'''select
                                    count(*) as total_trips
                                    , b.zone as dropoff_zone
                                from silver.fat_trips a
                                left join silver.dim_do_zones b on a.do_zone_key = b.do_zone_key
                                inner join silver.dim_calendar c on a.pickup_datetime::date = c.date
                                inner join silver.dim_services d on a.service_key = d.service_key
                                inner join silver.dim_vendors  e on a.vendor_key = e.vendor_key
                                where (c.year BETWEEN ({yearOption[0]}) and {yearOption[1]}) and d.service_desc IN ({', '.join(repr(e) for e in serviceOption)}) and e.vendor_desc IN ({', '.join(repr(e) for e in vendorOption)})
                                group by b.zone
                                order by 1 desc
                                limit 5''')

trips_by_services = getData(f'''select
                                    count(*) as total_trip
                                    , b.service_desc
                                    , concat(c.year, '.', c.month_name) as year_month
                                    , c.month
                                    , c.year
                                from silver.fat_trips a
                                inner join silver.dim_services b on a.service_key = b.service_key
                                inner join silver.dim_calendar c on a.pickup_datetime::date = c.date
                                inner join silver.dim_vendors  d on a.vendor_key = d.vendor_key
                                where (c.year BETWEEN ({yearOption[0]}) and {yearOption[1]}) and b.service_desc IN ({', '.join(repr(e) for e in serviceOption)}) and d.vendor_desc IN ({', '.join(repr(e) for e in vendorOption)})
                                group by b.service_desc, year_month, c.month, c.year
                                order by c.year, c.month;''')


################## Charts ######################

col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    st.plotly_chart(go.Figure(go.Indicator(title='Total Trips', mode="number", value = totals['total_trips'][0].round(2)) ).update_layout(height=250), config=config)

with col2:
    st.plotly_chart(go.Figure(go.Indicator(title='Total Passager', mode="number", value = totals['total_passenger'][0].round(2)) ).update_layout(height=250), config=config)

with col3:
    st.plotly_chart(go.Figure(go.Indicator(title='Total Amount', mode="number", number={'prefix':'$'} ,value = totals['total_amount'][0].round(2)) ).update_layout(height=250), config=config)

with col4:
    st.plotly_chart(go.Figure(go.Indicator(title='Avg Duration Trip', mode="number", number={'suffix':' Min'} ,value = totals['avg_trip_duration'][0].round(2)) ).update_layout(height=250), config=config)

with col5:
    st.plotly_chart(go.Figure(go.Indicator(title='Avg Fare per Trip', mode="number", number={'prefix':'$'} ,value = totals['avg_fare'][0].round(2)) ).update_layout(height=250), config=config)

with col6:
    st.plotly_chart(go.Figure(go.Indicator(title='Avg Trips Distance', mode="number", number={'suffix':' KM'} ,value = totals['avg_trip_distance'][0].round(2)) ).update_layout(height=250), config=config)

st.divider()

st.markdown("<h3 style='text-align: center;'>Monthly Trip Count by Service</h3>", unsafe_allow_html=True)
st.plotly_chart(px.line(trips_by_services, y='total_trip', x='year_month', color='service_desc', labels={"year_month":"Year - Month","total_trip":"Trips"}, color_discrete_map={ 'YELLOW': '#FFF449', 'GREEN': '#75FF49' }, markers=True), use_container_width=True, config=config)

col1, col2 = st.columns(2)

with col1:
    st.plotly_chart(px.pie(amout_by_year, title='Total Amount by Year', values='total_amount', names='year', labels={"vendor_desc":"Vendor","total_amount":"Amount"}).update_traces(textposition='inside', textinfo='percent+label'), use_container_width=True, config=config)
    st.plotly_chart(px.bar(avg_trip_distance, title='Quarter AVG Trip Distance', y='avg_trip_distance_km', x='quarter', color='year', barmode='group', text_auto='.2s', labels={"quarter":"Quarter","avg_trip_distance_km":"Distance (km)", "year":"Year"}), use_container_width=True, config=config)
    st.plotly_chart(px.bar(trips_by_zones_pu, title='Top 5 PickUp Zones', x='total_trips', y='pickup_zone',  color='pickup_zone', text_auto='.2s', labels={"pickup_zone":"Zone","total_trips":"Trips"}), use_container_width=True, config=config)
    st.plotly_chart(px.line(total_trips_by_yearmonth, title='Trip Count by Monthly', y='total_trips', x='year_month', markers=True, labels={"year_month":"Year - Month","total_trips":"Trips"}), use_container_width=True, config=config)
    st.plotly_chart(px.bar(total_trips_by_vendor, title='Trip Count by Vendor', y='total_trips', x='vendor_desc', color='vendor_desc', text_auto='.2s', labels={"vendor_desc":"Vendor","total_trips":"Trips"}), use_container_width=True, config=config)

with col2:
    st.plotly_chart(px.bar(amount_by_ratecodes, title='Total Amount by Rate Code',  y='avg_total_amount', x='ratecode_desc', color='ratecode_desc', text_auto='.2s', labels={"ratecode_desc":"Rate Code","avg_total_amount":"Amount"}), use_container_width=True, config=config)
    st.plotly_chart(px.line(avg_trip_duration, title='AVG Trip Duration by Week', y='avg_trip_duration_min', x='day_of_week_name', markers=True, labels={"day_of_week_name":"Day","avg_trip_duration_min":"Duration (min)"}), use_container_width=True, config=config)
    st.plotly_chart(px.bar(trips_by_zones_do, title='Top 5 Drop Off Zones', x='total_trips', y='dropoff_zone', color='dropoff_zone', orientation='h', text_auto='.2s', labels={"dropoff_zone":"Zone","total_trips":"Trips"}), use_container_width=True, config=config)
    st.plotly_chart(px.area(total_trips_by_weak, title='Total Trip by Week', y='total_trips', x='day_of_week_name', markers=True, labels={"day_of_week_name":"Day","total_trips":"Trips"}), use_container_width=True, config=config)
    st.plotly_chart(px.pie(trips_by_payments_type, title='Trip Count by Payment' ,values='total_trips', names='payment_desc', hole=.3).update_traces(textinfo='percent+label'), use_container_width=True, config=config)
