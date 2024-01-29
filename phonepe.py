import os
import json
import streamlit as st
import pandas as pd
import plotly.express as px
import mysql.connector
from streamlit_option_menu import option_menu
from sqlalchemy import create_engine
from sqlalchemy import text

#Connecting to MySQL
engine = create_engine('mysql+mysqlconnector://root:<password>@localhost/phonepe')
connection = engine.connect()

#Cloning data from PhonePe Pulse github repository:
#git clone https://github.com/PhonePe/pulse.git 
#above command passed from terminal and data is saved to local project repository'''

#Extracting aggregated data
def agg_data():
    #Extracting aggregated data - transaction:
    agg_trans_path = "C:/Phonepe/pulse/data/aggregated/transaction/country/india/state/"
    agg_trans_st_lst = os.listdir(agg_trans_path)
    agg_trans_clms = {'state': [], 'year': [], 'quarter': [], 'payment_category': [], 'count':[], 'amount':[]}
    for i in agg_trans_st_lst:
        agg_trans_st_yr_lst = os.listdir(agg_trans_path+i+'/')
        for j in agg_trans_st_yr_lst:
            agg_trans_st_yr_qtr_lst = os.listdir(agg_trans_path+i+'/'+j+'/')
            for k in agg_trans_st_yr_qtr_lst:
                unit_file = agg_trans_path+i+'/'+j+'/'+k
                unit_data = open(unit_file, 'r')
                data = json.load(unit_data)
                for l in data['data']['transactionData']:
                    agg_trans_clms['state'].append(i.replace('-',' ').title())
                    agg_trans_clms['year'].append(j)
                    agg_trans_clms['quarter'].append(int(k.strip('.json')))
                    agg_trans_clms['payment_category'].append(l['name'])
                    agg_trans_clms['count'].append(l['paymentInstruments'][0]['count'])
                    agg_trans_clms['amount'].append(l['paymentInstruments'][0]['amount'])
    df_agg_trans = pd.DataFrame(agg_trans_clms)
    
    #Extracting aggregated data - user:
    agg_user_path = "C:/Phonepe/pulse/data/aggregated/user/country/india/state/"
    agg_user_st_lst = os.listdir(agg_user_path)
    agg_user_clms_1 = {'state': [], 'year': [], 'quarter': [], 'brand': [], 'count':[], 'percentage':[]}
    agg_user_clms_2 = {'state': [], 'year': [], 'quarter': [], 'registered_users': [], 'app_opens':[]}
    for i in agg_user_st_lst:
        agg_user_st_yr_lst = os.listdir(agg_user_path+i+'/')
        for j in agg_user_st_yr_lst:
            agg_trans_st_yr_qtr_lst = os.listdir(agg_user_path+i+'/'+j+'/')
            for k in agg_trans_st_yr_qtr_lst:
                unit_file = agg_user_path+i+'/'+j+'/'+k
                unit_data = open(unit_file, 'r')
                data = json.load(unit_data)
                agg_user_clms_2['state'].append(i.replace('-',' ').title())
                agg_user_clms_2['year'].append(j)
                agg_user_clms_2['quarter'].append(int(k.strip('.json')))
                agg_user_clms_2['registered_users'].append(data['data']['aggregated']['registeredUsers'])
                agg_user_clms_2['app_opens'].append(data['data']['aggregated']['appOpens'])
                try:
                    for l in data['data']['usersByDevice']:
                        agg_user_clms_1['state'].append(i.replace('-',' ').title())
                        agg_user_clms_1['year'].append(j)
                        agg_user_clms_1['quarter'].append(int(k.strip('.json')))
                        agg_user_clms_1['brand'].append(l['brand'])
                        agg_user_clms_1['count'].append(l['count'])
                        agg_user_clms_1['percentage'].append(l['percentage'])
                except:
                    pass
    df_agg_user_1 = pd.DataFrame(agg_user_clms_1)
    df_agg_user_2 = pd.DataFrame(agg_user_clms_2)

    #Extracting aggregated data - insurance:
    agg_ins_path = "C:/Phonepe/pulse/data/aggregated/insurance/country/india/state/"
    agg_ins_st_lst = os.listdir(agg_ins_path)
    agg_ins_clms = {'state': [], 'year': [], 'quarter': [], 'count':[], 'amount':[]}
    for i in agg_ins_st_lst:
        agg_ins_st_yr_lst = os.listdir(agg_ins_path+i+'/')
        for j in agg_ins_st_yr_lst:
            agg_ins_st_yr_qtr_lst = os.listdir(agg_ins_path+i+'/'+j+'/')
            for k in agg_ins_st_yr_qtr_lst:
                unit_file = agg_ins_path+i+'/'+j+'/'+k
                unit_data = open(unit_file, 'r')
                data = json.load(unit_data)
                for l in data['data']['transactionData']:
                    agg_ins_clms['state'].append(i.replace('-',' ').title())
                    agg_ins_clms['year'].append(j)
                    agg_ins_clms['quarter'].append(int(k.strip('.json')))
                    agg_ins_clms['count'].append(l['paymentInstruments'][0]['count'])
                    agg_ins_clms['amount'].append(l['paymentInstruments'][0]['amount'])
    df_agg_ins = pd.DataFrame(agg_ins_clms)

    return df_agg_trans, df_agg_user_1, df_agg_user_2, df_agg_ins

#Extracting map data
def map_data():
    #Extracting map data - transaction:
    map_trans_path = "C:/Phonepe/pulse/data/map/transaction/hover/country/india/state/"
    map_trans_st_lst = os.listdir(map_trans_path)
    map_trans_clms = {'state': [], 'year': [], 'quarter': [], 'district': [], 'count':[], 'amount':[]}
    for i in map_trans_st_lst:
        map_trans_st_yr_lst = os.listdir(map_trans_path+i+'/')
        for j in map_trans_st_yr_lst:
            map_trans_st_yr_qtr_lst = os.listdir(map_trans_path+i+'/'+j+'/')
            for k in map_trans_st_yr_qtr_lst:
                unit_file = map_trans_path+i+'/'+j+'/'+k
                unit_data = open(unit_file, 'r')
                data = json.load(unit_data)
                for l in data['data']['hoverDataList']:
                    map_trans_clms['state'].append(i.replace('-',' ').title())
                    map_trans_clms['year'].append(j)
                    map_trans_clms['quarter'].append(int(k.strip('.json')))
                    map_trans_clms['district'].append(l['name'])
                    map_trans_clms['count'].append(l['metric'][0]['count'])
                    map_trans_clms['amount'].append(l['metric'][0]['amount'])
    df_map_trans = pd.DataFrame(map_trans_clms)

    #Extracting map data - user:
    map_user_path = "C:/Phonepe/pulse/data/map/user/hover/country/india/state/"
    map_user_st_lst = os.listdir(map_user_path)
    map_user_clms = {'state': [], 'year': [], 'quarter': [], 'district': [], 'registered_users':[], 'app_opens':[]}
    for i in map_user_st_lst:
        map_user_st_yr_lst = os.listdir(map_user_path+i+'/')
        for j in map_user_st_yr_lst:
            map_trans_st_yr_qtr_lst = os.listdir(map_user_path+i+'/'+j+'/')
            for k in map_trans_st_yr_qtr_lst:
                unit_file = map_user_path+i+'/'+j+'/'+k
                unit_data = open(unit_file, 'r')
                data = json.load(unit_data)
                for l in data['data']['hoverData'].items():
                    map_user_clms['state'].append(i.replace('-',' ').title())
                    map_user_clms['year'].append(j)
                    map_user_clms['quarter'].append(int(k.strip('.json')))
                    map_user_clms['district'].append(l[0])
                    map_user_clms['registered_users'].append(l[1]['registeredUsers'])
                    map_user_clms['app_opens'].append(l[1]['appOpens'])
    df_map_user = pd.DataFrame(map_user_clms)

    #Extracting map data - insurance:
    map_ins_path = "C:/Phonepe/pulse/data/map/insurance/hover/country/india/state/"
    map_ins_st_lst = os.listdir(map_ins_path)
    map_ins_clms = {'state': [], 'year': [], 'quarter': [], 'district':[], 'count':[], 'amount':[]}
    for i in map_ins_st_lst:
        map_ins_st_yr_lst = os.listdir(map_ins_path+i+'/')
        for j in map_ins_st_yr_lst:
            agg_ins_st_yr_qtr_lst = os.listdir(map_ins_path+i+'/'+j+'/')
            for k in agg_ins_st_yr_qtr_lst:
                unit_file = map_ins_path+i+'/'+j+'/'+k
                unit_data = open(unit_file, 'r')
                data = json.load(unit_data)
                for l in data['data']['hoverDataList']:
                    map_ins_clms['state'].append(i.replace('-',' ').title())
                    map_ins_clms['year'].append(j)
                    map_ins_clms['quarter'].append(int(k.strip('.json')))
                    map_ins_clms['district'].append(l['name'])
                    map_ins_clms['count'].append(l['metric'][0]['count'])
                    map_ins_clms['amount'].append(l['metric'][0]['amount'])
    df_map_ins = pd.DataFrame(map_ins_clms)

    return df_map_trans, df_map_user, df_map_ins

#Extracting top data
def top_data():
    #Extracting top data - transaction:
    top_trans_path = "C:/Phonepe/pulse/data/top/transaction/country/india/state/"
    top_trans_st_lst = os.listdir(top_trans_path)
    top_trans_clms_district = {'state': [], 'year': [], 'quarter': [], 'district': [], 'count':[], 'amount':[]}
    top_trans_clms_pincode = {'state': [], 'year': [], 'quarter': [], 'pincode': [], 'count':[], 'amount':[]}
    for i in top_trans_st_lst:
        top_trans_st_yr_lst = os.listdir(top_trans_path+i+'/')
        for j in top_trans_st_yr_lst:
            top_trans_st_yr_qtr_lst = os.listdir(top_trans_path+i+'/'+j+'/')
            for k in top_trans_st_yr_qtr_lst:
                unit_file = top_trans_path+i+'/'+j+'/'+k
                unit_data = open(unit_file, 'r')
                data = json.load(unit_data)
                for l in data['data']['districts']:
                    top_trans_clms_district['state'].append(i.replace('-',' ').title())
                    top_trans_clms_district['year'].append(j)
                    top_trans_clms_district['quarter'].append(int(k.strip('.json')))
                    top_trans_clms_district['district'].append(l['entityName'])
                    top_trans_clms_district['count'].append(l['metric']['count'])
                    top_trans_clms_district['amount'].append(l['metric']['amount'])
                for l in data['data']['pincodes']:
                    top_trans_clms_pincode['state'].append(i.replace('-',' ').title())
                    top_trans_clms_pincode['year'].append(j)
                    top_trans_clms_pincode['quarter'].append(int(k.strip('.json')))
                    top_trans_clms_pincode['pincode'].append(l['entityName'])
                    top_trans_clms_pincode['count'].append(l['metric']['count'])
                    top_trans_clms_pincode['amount'].append(l['metric']['amount'])
    df_top_trans_district = pd.DataFrame(top_trans_clms_district)
    df_top_trans_pincode = pd.DataFrame(top_trans_clms_pincode)

    #Extracting top data - user:
    top_user_path = "C:/Phonepe/pulse/data/top/user/country/india/state/"
    top_user_st_lst = os.listdir(top_user_path)
    top_user_clms_district = {'state': [], 'year': [], 'quarter': [], 'district': [], 'registered_users':[]}
    top_user_clms_pincode = {'state': [], 'year': [], 'quarter': [], 'pincode': [], 'registered_users':[]}
    for i in top_user_st_lst:
        top_user_st_yr_lst = os.listdir(top_user_path+i+'/')
        for j in top_user_st_yr_lst:
            top_trans_st_yr_qtr_lst = os.listdir(top_user_path+i+'/'+j+'/')
            for k in top_trans_st_yr_qtr_lst:
                unit_file = top_user_path+i+'/'+j+'/'+k
                unit_data = open(unit_file, 'r')
                data = json.load(unit_data)
                for l in data['data']['districts']:
                    top_user_clms_district['state'].append(i.replace('-',' ').title())
                    top_user_clms_district['year'].append(j)
                    top_user_clms_district['quarter'].append(int(k.strip('.json')))
                    top_user_clms_district['district'].append(l['name'])
                    top_user_clms_district['registered_users'].append(l['registeredUsers'])
                for l in data['data']['pincodes']:
                    top_user_clms_pincode['state'].append(i.replace('-',' ').title())
                    top_user_clms_pincode['year'].append(j)
                    top_user_clms_pincode['quarter'].append(int(k.strip('.json')))
                    top_user_clms_pincode['pincode'].append(l['name'])
                    top_user_clms_pincode['registered_users'].append(l['registeredUsers'])
    df_top_user_district = pd.DataFrame(top_user_clms_district)
    df_top_user_pincode = pd.DataFrame(top_user_clms_pincode)

    #Extracting top data - insurance:
    top_ins_path = "C:/Phonepe/pulse/data/top/insurance/country/india/state/"
    top_ins_st_lst = os.listdir(top_ins_path)
    top_ins_clms_district = {'state': [], 'year': [], 'quarter': [], 'district': [], 'count':[], 'amount':[]}
    top_ins_clms_pincode = {'state': [], 'year': [], 'quarter': [], 'pincode': [], 'count':[], 'amount':[]}
    for i in top_ins_st_lst:
        top_ins_st_yr_lst = os.listdir(top_ins_path+i+'/')
        for j in top_ins_st_yr_lst:
            top_ins_st_yr_qtr_lst = os.listdir(top_ins_path+i+'/'+j+'/')
            for k in top_ins_st_yr_qtr_lst:
                unit_file = top_ins_path+i+'/'+j+'/'+k
                unit_data = open(unit_file, 'r')
                data = json.load(unit_data)
                for l in data['data']['districts']:
                    top_ins_clms_district['state'].append(i.replace('-',' ').title())
                    top_ins_clms_district['year'].append(j)
                    top_ins_clms_district['quarter'].append(int(k.strip('.json')))
                    top_ins_clms_district['district'].append(l['entityName'])
                    top_ins_clms_district['count'].append(l['metric']['count'])
                    top_ins_clms_district['amount'].append(l['metric']['amount'])
                for l in data['data']['pincodes']:
                    top_ins_clms_pincode['state'].append(i.replace('-',' ').title())
                    top_ins_clms_pincode['year'].append(j)
                    top_ins_clms_pincode['quarter'].append(int(k.strip('.json')))
                    top_ins_clms_pincode['pincode'].append(l['entityName'])
                    top_ins_clms_pincode['count'].append(l['metric']['count'])
                    top_ins_clms_pincode['amount'].append(l['metric']['amount'])
    df_top_ins_district = pd.DataFrame(top_ins_clms_district)
    df_top_ins_pincode = pd.DataFrame(top_ins_clms_pincode)

    return df_top_trans_district, df_top_trans_pincode, df_top_user_district, df_top_user_pincode, df_top_ins_district, df_top_ins_pincode

#Extracting data:
df_agg_trans, df_agg_user_1, df_agg_user_2, df_agg_ins = agg_data()
df_map_trans, df_map_user, df_map_ins = map_data()
df_top_trans_district, df_top_trans_pincode, df_top_user_district, df_top_user_pincode, df_top_ins_district, df_top_ins_pincode = top_data()

#Data check:
# df_agg_trans.info()
# df_agg_user_1.info()
# df_agg_user_2.info()
# df_agg_ins.info()
# df_map_trans.info()
# df_map_user.info()
# df_map_ins.info()
# df_top_trans_district.info()
# df_top_trans_pincode.info()
# df_top_user_district.info()
# df_top_user_pincode.info()
# df_top_ins_district.info()
# df_top_ins_pincode.info()

#Migrating to SQL:
df_agg_trans.to_sql('aggregated_transaction', connection, index = False, if_exists='replace')
df_agg_user_1.to_sql('aggregated_user_brands', connection, if_exists='replace')
df_agg_user_2.to_sql('aggregated_user_registered', connection, if_exists='replace')
df_agg_ins.to_sql('aggregated_insurance', connection, if_exists='replace')
df_map_trans.to_sql('map_transaction', connection, if_exists='replace')
df_map_user.to_sql('map_user', connection, if_exists='replace')
df_map_ins.to_sql('map_insurance', connection, if_exists='replace')
df_top_trans_district.to_sql('top_transaction_district', connection, if_exists='replace')
df_top_trans_pincode.to_sql('top_transaction_pincode', connection, if_exists='replace')
df_top_user_district.to_sql('top_user_district', connection, if_exists='replace')
df_top_user_pincode.to_sql('top_user_pincode', connection, if_exists='replace')
df_top_ins_district.to_sql('top_insurance_district', connection, if_exists='replace')
df_top_ins_pincode.to_sql('top_insurance_pincode', connection, if_exists='replace')


#GeoJson file
india_states = json.load(open('st_india.geojson','r'))
state_list= [feature["properties"]["ST_NM"] for feature in india_states["features"]]
state_list.sort()

# #Building streamlit dashboard:
st.set_page_config(layout= "wide")
with st.sidebar:
    st.title('PHONEPE PULSE DATA VISUALISATION AND EXPLORATION')
    st.image('img1.jpg')
    select = option_menu(
        menu_title = "Home",
        options = ['About PhonePe pulse data','Data Insights & Visualisation'],
        default_index = 0,
    )
    st.subheader('Technologies used:')
    st.caption('Python')
    st.caption('MySQL')
    st.caption('Streamlit')
    st.caption('plotly')
    st.caption('Git')

if select == 'About PhonePe pulse data':
        st.divider()
        st.markdown("<h1 style='text-align: center; color: black;'>PHONEPE PULSE DATA</h1>", unsafe_allow_html=True)
        st.divider()
        st.markdown('<style>div.block-container{padding-top:1rem;}</style>',unsafe_allow_html=True)
        st.subheader("PhonePe started in 2016 has been revolutionising digital payments in India")
        st.write("From the largest towns to the remotest villages, there is a payments revolution being driven by the penetration of mobile phones, mobile internet ")
        st.write("PhonePe continues to be the biggest player in digital payment category")
        st.write("Simple, fast and secure and single app for multiple needs")
        st.divider()
        st.subheader("pulse data")
        st.write("PhonePe pulse data gives us anonymised aggregate data sets of digital payments in india")
        st.markdown("##### With this data we can arrive at some intresting insights and trends")
        st.image('img2.jpg')
        
if select == 'Data Insights & Visualisation':
    st.markdown('<style>div.block-container{padding-top:1rem;}</style>',unsafe_allow_html=True)
    st.image('img3.jpg')
    query = st.selectbox('select an insight',
        ('select',
        'Growth in registered users over the years',
        'Preffered phone brands from 2018 to 2022',
        'Legend of state transactions from 2018 to 2023',
        'App usage for different payment categories',
        'App usage for insurance payment',
        'Top 10 transaction - districts',
        'Top 10 registred users - districts',
        'Top 10 insurance payments - districts',
        'Top 10 app opens',
        'Least transaction states'
        )
    )

    if query == 'Growth in registered users over the years':
        resi = connection.execute(text('''SELECT CONCAT(year, ' ', 'Qtr', quarter) AS year, SUM(registered_users) AS registered_users 
                                       FROM phonepe.aggregated_user_registered GROUP BY year, quarter'''))
        qdfi = pd.DataFrame(resi)
        figi = px.line(qdfi, x='year', y='registered_users', title = 'Growth in Registered Users in India')
        figi.update_layout(xaxis_title = 'Year', yaxis_title = 'Registered Users')
        figi.update_xaxes(tickangle=300)
        st.write(figi)
        res = connection.execute(text('''SELECT DISTINCT state FROM phonepe.aggregated_user_registered'''))
        qdf = pd.DataFrame(res)
        lst = qdf['state'].tolist()
        lst.insert(0,'')
        selected_state = st.selectbox('select a state',(lst))
        if selected_state:
            query = f"SELECT CONCAT(year, ' ', 'Qtr', quarter) AS year, registered_users FROM phonepe.aggregated_user_registered WHERE state = '{selected_state}' order by year, quarter"
            resp = connection.execute(text(query))
            qdff = pd.DataFrame(resp)
            fig = px.line(qdff, x='year', y='registered_users', title = f'Growth in Registered Users  - {selected_state}')
            fig.update_layout(xaxis_title = 'Year', yaxis_title = 'Registered Users')
            fig.update_xaxes(tickangle=300)
            st.write(fig)

    if query == 'Preffered phone brands from 2018 to 2022':
        res = connection.execute(text('''SELECT brand, SUM(count) AS count FROM phonepe.aggregated_user_brands GROUP BY brand'''))
        qdf = pd.DataFrame(res)
        qdf.replace(to_replace = ['Lenovo','Tecno','Micromax','Infinix','Asus','Gionee','Lava','HMD Global','Lyf','COOLPAD'], value = 'Others',inplace=True)
        fig = px.pie(qdf, values='count', names='brand')
        fig.update_layout(xaxis_title = 'No of phones', yaxis_title = 'Brand')
        st.write(fig)

    if query == 'Legend of state transactions from 2018 to 2023':
        res = connection.execute(text('''SELECT SUM(amount) AS amount, state FROM phonepe.aggregated_transaction GROUP BY state'''))
        qdf = pd.DataFrame(res)
        fig = px.choropleth(qdf, geojson= india_states, locations= 'state', featureidkey= 'properties.ST_NM',
                                color= 'amount', color_continuous_scale= 'Viridis',
                                range_color= (qdf['amount'].min(),qdf['amount'].max()),
                                hover_name= 'state',title = 'overall',
                                fitbounds= 'locations',width =1000, height= 1000)
        fig.update_geos(visible =False)
        st.write(fig)
        selected_year = st.selectbox('select a year',['2018','2019','2020','2021','2022','2023'])
        if selected_year:
            query = f"SELECT SUM(amount) AS amount, state FROM phonepe.aggregated_transaction WHERE year = {selected_year} GROUP BY state"
            resp = connection.execute(text(query))
            qdff = pd.DataFrame(resp)
            figf = px.choropleth(qdff, geojson= india_states, locations= 'state', featureidkey= 'properties.ST_NM',
                                    color= 'amount', color_continuous_scale= 'Viridis',
                                    range_color= (qdff['amount'].min(),qdff['amount'].max()),
                                    hover_name= 'state',title = f"{selected_year}",
                                    fitbounds= 'locations',width =1000, height= 1000)
            figf.update_geos(visible =False)
            st.write(figf)

    if query == 'App usage for different payment categories':
        res = connection.execute(text('''SELECT payment_category, SUM(amount) AS amount FROM phonepe.aggregated_transaction GROUP BY payment_category'''))
        qdf = pd.DataFrame(res)
        fig = px.bar(qdf, x='amount', y='payment_category', orientation='h', color= 'payment_category', log_x=True)
        fig.update_layout(xaxis_title = 'Amount', yaxis_title = 'Payment Category')
        fig.update_layout(showlegend=False)
        st.write(fig)

    if query == 'App usage for insurance payment':
        res = connection.execute(text('''SELECT SUM(count) AS count, SUM(amount) AS amount, state FROM phonepe.aggregated_insurance GROUP BY state'''))
        qdf = pd.DataFrame(res)
        fig = px.scatter(qdf, x='amount',y='count', log_x=True, log_y=True, width =1300, height= 800, hover_data=['state'], color='state')
        fig.update_traces(marker=dict(size=15))
        fig.update_layout(showlegend=False)
        st.write(fig)

    if query == 'Top 10 transaction - districts':
        res = connection.execute(text('''SELECT state, district, SUM(amount) AS total_amount FROM phonepe.top_transaction_district GROUP BY district ORDER BY total_amount DESC LIMIT 10'''))
        qdf = pd.DataFrame(res)
        fig = px.bar(qdf, x='district', y='total_amount', color= 'state')
        fig.update_layout(xaxis_title = 'District', yaxis_title = 'Amount')
        fig.update_xaxes(tickangle=300)
        st.write(fig)

    if query == 'Top 10 registred users - districts':
        res = connection.execute(text('''SELECT state, district, SUM(registered_users) AS total_reg_users FROM phonepe.top_user_district GROUP BY district ORDER BY total_reg_users DESC LIMIT 10'''))
        qdf = pd.DataFrame(res)
        fig = px.bar(qdf, x='district', y='total_reg_users', color= 'state')
        fig.update_layout(xaxis_title = 'District', yaxis_title = 'Registered Users')
        fig.update_xaxes(tickangle=300)
        st.write(fig)

    if query == 'Top 10 insurance payments - districts':
        res = connection.execute(text('''SELECT state, district, SUM(amount) AS total_amount FROM phonepe.top_insurance_district GROUP BY district ORDER BY total_amount DESC LIMIT 10'''))
        qdf = pd.DataFrame(res)
        fig = px.bar(qdf, x='district', y='total_amount', color= 'state')
        fig.update_layout(xaxis_title = 'District', yaxis_title = 'Insurance payments')
        fig.update_xaxes(tickangle=300)
        st.write(fig)

    if query == 'Top 10 app opens':
        res = connection.execute(text('''SELECT state, sum(app_opens) AS total_app_opens FROM phonepe.aggregated_user_registered GROUP BY state ORDER BY total_app_opens DESC LIMIT 10'''))
        qdf = pd.DataFrame(res)
        fig = px.bar(qdf, x='state', y='total_app_opens', color= 'state')
        fig.update_layout(xaxis_title = 'State', yaxis_title = 'App Opens')
        fig.update_xaxes(tickangle=300)
        fig.update_layout(showlegend=False)
        st.write(fig)

    if query == 'Least transaction states':
        res = connection.execute(text('''SELECT state, SUM(amount) as total_amount FROM phonepe.aggregated_transaction GROUP BY state ORDER BY total_amount LIMIT 10'''))
        qdf = pd.DataFrame(res)
        fig = px.bar(qdf, x='state', y='total_amount', color= 'state')
        fig.update_layout(xaxis_title = 'State', yaxis_title = 'Amount')
        fig.update_xaxes(tickangle=300)
        fig.update_layout(showlegend=False)
        st.write(fig)