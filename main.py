import mysql.connector
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
import json
import pandas as pd
import mysql.connector
import os
import re
from sqlalchemy import create_engine

class phonepe:

    def __init__(self):
        self.db = 'phonepe'
        self.host = "localhost"
        self.user = "root"


    # MySQL DB connect
    def mysql_db_connect(self):
        try:
            self.db_mysql_cnx = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password="sumit",
                # Replace with your actual password
                database=self.db
            )
            self.mysql_cursor = self.db_mysql_cnx.cursor()
            print("Connected to MySQL database")
        except mysql.connector.Error as err:
            print(f"Error mysql_db_connect - {err}")

    # get all states
    def get_states(self):

        trans_query = "SELECT distinct(state) FROM agg_trans order by state asc"
        self.mysql_cursor.execute(trans_query)

        # Fetch all the rows returned by the query
        trans_rows = self.mysql_cursor.fetchall()

        # Get the column names from the cursor description
        states = [st[0] for st in trans_rows]

        return states

    # get all year
    def get_year(self):

        trans_query = "SELECT distinct(year) FROM agg_trans order by year asc"
        self.mysql_cursor.execute(trans_query)

        # Fetch all the rows returned by the query
        trans_rows = self.mysql_cursor.fetchall()

        # Get the column names from the cursor description
        yr = [st[0] for st in trans_rows]

        return yr

    # get all modes
    def get_mode(self):

        trans_query = "SELECT distinct(metric) FROM agg_trans"
        self.mysql_cursor.execute(trans_query)

        # Fetch all the rows returned by the query
        trans_rows = self.mysql_cursor.fetchall()

        # Get the column names from the cursor description
        mode = [st[0] for st in trans_rows]

        return mode

    # get DF for Transaction data for State and mode selected
    def get_transaction_data(self, state, mode):

        trans_query = "SELECT count,concat(year,'-Q',quarter) as year  from agg_trans where metric=\'{0}\' and state=\'{1}\' order by year asc".format(
            mode, state)
        self.mysql_cursor.execute(trans_query)

        # Fetch all the rows returned by the query
        trans_rows = self.mysql_cursor.fetchall()

        # Get the column names from the cursor description
        columns = [desc[0].upper() for desc in self.mysql_cursor.description]

        # Create a Pandas DataFrame from the query result
        df = pd.DataFrame(trans_rows, columns=columns)
        return df

    # get DF for Transaction amount for State and mode selected
    def get_transaction_amount(self, state, mode):

        trans_query = "SELECT amount,year,quarter from agg_trans where metric=\'{0}\' and state=\'{1}\' order by year asc".format(
            mode, state)
        self.mysql_cursor.execute(trans_query)

        # Fetch all the rows returned by the query
        trans_rows = self.mysql_cursor.fetchall()

        # Get the column names from the cursor description
        columns = [desc[0].upper() for desc in self.mysql_cursor.description]

        # Create a Pandas DataFrame from the query result
        df = pd.DataFrame(trans_rows, columns=columns)

        return df

        # get DF for Transaction amount for State and yr, qtr selected

    def get_district_transaction(self, state, yr, qtr):

        trans_query = "SELECT metric as district, count, amount from top_trans where state=\'{0}\' and year={1} and quarter={2} order by year asc".format(
            state, yr, qtr)
        self.mysql_cursor.execute(trans_query)

        # Fetch all the rows returned by the query
        trans_rows = self.mysql_cursor.fetchall()

        # Get the column names from the cursor description
        columns = [desc[0].upper() for desc in self.mysql_cursor.description]

        # Create a Pandas DataFrame from the query result
        df = pd.DataFrame(trans_rows, columns=columns)

        return df

        # get DF for Transaction amount for mode and year selected

    def get_yr_transaction(self, mode, yr):

        trans_query = "SELECT state, sum(count) count from agg_trans where year={0} and metric=\'{1}\' group by state".format(
            yr, mode)
        self.mysql_cursor.execute(trans_query)

        # Fetch all the rows returned by the query
        trans_rows = self.mysql_cursor.fetchall()

        # Get the column names from the cursor description
        columns = [desc[0].upper() for desc in self.mysql_cursor.description]

        # Create a Pandas DataFrame from the query result
        df = pd.DataFrame(trans_rows, columns=columns)

        return df

        # get DF for Transaction amount for mode and year selected

    def get_overall_trans(self):

        trans_query = "SELECT sum(count) count, year from agg_trans group by year"
        self.mysql_cursor.execute(trans_query)

        # Fetch all the rows returned by the query
        trans_rows = self.mysql_cursor.fetchall()

        # Get the column names from the cursor description
        columns = [desc[0].upper() for desc in self.mysql_cursor.description]

        # Create a Pandas DataFrame from the query result
        df = pd.DataFrame(trans_rows, columns=columns)

        return df

        # get DF for User data for State and mode selected

    def get_user_data_statewise(self, state):

        trans_query = "SELECT sum(registeredUsers) registeredusers,sum(appOpens) appopens,year from map_user where state=\'{0}\'  group by year".format(
            state)
        self.mysql_cursor.execute(trans_query)

        # Fetch all the rows returned by the query
        trans_rows = self.mysql_cursor.fetchall()

        # Get the column names from the cursor description
        columns = [desc[0].upper() for desc in self.mysql_cursor.description]

        # Create a Pandas DataFrame from the query result
        df = pd.DataFrame(trans_rows, columns=columns)

        return df

    # get DF for User data for State and mode selected
    def get_user_brand_statewise(self, state, yr):

        trans_query = "SELECT metric as brand,sum(count) count from agg_user where state=\'{0}\' and year = {1} group by metric".format(
            state, yr)
        self.mysql_cursor.execute(trans_query)

        # Fetch all the rows returned by the query
        trans_rows = self.mysql_cursor.fetchall()

        # Get the column names from the cursor description
        columns = [desc[0].upper() for desc in self.mysql_cursor.description]

        # Create a Pandas DataFrame from the query result
        df = pd.DataFrame(trans_rows, columns=columns)

        return df

    # get DF for User data for State and mode selected
    def get_map_user_statewise(self):

        trans_query = "SELECT state, sum(registeredUsers) as 'registered users' from map_user group by state"
        self.mysql_cursor.execute(trans_query)

        # Fetch all the rows returned by the query
        trans_rows = self.mysql_cursor.fetchall()

        # Get the column names from the cursor description
        columns = [desc[0] for desc in self.mysql_cursor.description]

        # Create a Pandas DataFrame from the query result
        df = pd.DataFrame(trans_rows, columns=columns)

        df['state'] = df['state'].str.title()
        df['state'] = df['state'].str.replace('-', ' ')
        df['state'] = df['state'].str.replace('Andaman & Nicobar Islands',
                                              'Andaman & Nicobar')
        df['state'] = df['state'].str.replace(
            'Dadra & Nagar Haveli & Daman & Diu',
            'Dadra and Nagar Haveli and Daman and Diu')

        df = df.astype({'registered users': 'int'})

        return df


###################### Main Streamlit Application starts here #####################

def main():
    logo_image = "phonepe-logo.png"  # Replace with the actual path to your logo image
    st.image(logo_image, width=100)

    st.markdown('<style>h1{color: #6739B7;}</style>', unsafe_allow_html=True)
    st.title("Phonepe Pulse Data Visualization and Exploration")
    st.markdown('<h3 style="color: #E1C16E;">By Sumit Kumar</h3>',unsafe_allow_html=True)


    pp_dash = phonepe()
    pp_dash.mysql_db_connect()

    m = st.markdown("""
                    <style>
                    div.stButton > button:first-child {
                        background-color: #0A6EBD;
                        color:#ffffff;
                    }


                    </style>""", unsafe_allow_html=True)

    states = pp_dash.get_states()
    yr = pp_dash.get_year()
    st.session_state.pp_dash = pp_dash

    mode = pp_dash.get_mode()

    state_selected = st.sidebar.selectbox(
        "Choose a State",
        states
    )



    yr_selected = st.sidebar.selectbox(
        "Choose a Year",
        (yr)
    )

    qtr_selected = st.sidebar.selectbox(
        "Choose a Quarter",
        (1, 2, 3, 4)
    )

    mode_selected = st.sidebar.selectbox(
        "Choose a Transaction Mode",
        (mode)
    )

    st.sidebar.markdown(
        '<style> .st-dz button { color: #6739B7; }</style>',
        unsafe_allow_html=True
    )
    if st.sidebar.button("Analyse & Visualise"):
        pp_dash.state_selected = state_selected
        pp_dash.mode_selected = mode_selected
        pp_dash.yr_selected = yr_selected
        pp_dash.qtr_selected = qtr_selected
        visualise()

    pp_dash.mysql_cursor.close()
    pp_dash.db_mysql_cnx.close()


def visualise():
    tab1, tab2, tab3, tab4 = st.tabs(
        ["GeoMap", "Transaction Data", "User Data", "Overall Analysis"])

    if "pp_dash" in st.session_state:
        pp_dash = st.session_state.pp_dash

        with tab1:
            ###### Geomap

            map_user_state_df = pp_dash.get_map_user_statewise()

            # Create scatter map
            map_user_state_df.sort_values(by=['state'])
            geo_regusers_fig = px.choropleth(
                map_user_state_df,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',
                locations='state',
                color_continuous_scale='Reds',
                color='registered users',
                title="<span style='color: #6739B7;'>Insight on PhonePe Registered users across Indian States</span>",
                height=800,
                width=800
            )


            geo_regusers_fig.update_geos(fitbounds="locations",
                                         visible=False, )
            geo_regusers_fig.update_layout(
                autosize=False,
                margin=dict(
                    l=20,
                    r=20,
                    b=20,
                    t=20,

                    autoexpand=True
                ),
                width=800,
                height=400,
            )
            st.plotly_chart(geo_regusers_fig, theme="streamlit",
                            use_container_width=True,
                            title="<span style='color: #6739B7;'>Total Registered users across Indian States</span>")

        with tab2:
            title = pp_dash.state_selected.upper().replace('-', ' ')

            st.subheader(title)

            ####### Transaction Count analysis
            ch_title = 'Total Count of ' + pp_dash.mode_selected + ' Transactions per Year-Quarter'
            trans_count_df = pp_dash.get_transaction_data(
                pp_dash.state_selected, pp_dash.mode_selected)

            trans_fig = px.bar(trans_count_df, x="YEAR", y="COUNT",
                               color="COUNT",
                               hover_data=['COUNT'], barmode='stack',
                               title=ch_title)

            st.plotly_chart(trans_fig, theme="streamlit",
                            use_container_width=True)

            ###### Transaction amount
            ch_title = 'Total Amount of ' + pp_dash.mode_selected + ' Transactions per Year-Quarter'
            trans_amount_df = pp_dash.get_transaction_amount(
                pp_dash.state_selected, pp_dash.mode_selected)

            trans_amount_fig = px.bar(trans_amount_df, x="YEAR", y="AMOUNT",
                                      color="QUARTER",
                                      hover_data=['AMOUNT'], barmode='stack',
                                      title=ch_title)

            st.plotly_chart(trans_amount_fig)

            ###### District count
            ch_title = "District wise all Transactions for Year " + pp_dash.yr_selected + ' Q' + str(
                pp_dash.qtr_selected)
            dist_count_df = pp_dash.get_district_transaction(
                pp_dash.state_selected, pp_dash.yr_selected,
                pp_dash.qtr_selected)

            dist_count_df['DISTRICT'] = dist_count_df[
                'DISTRICT'].str.capitalize()
            dist_fig = px.pie(dist_count_df, names="DISTRICT", values="COUNT",
                              color="COUNT",
                              hover_data=['AMOUNT'], title=ch_title)

            st.plotly_chart(dist_fig)

        with tab3:
            title = pp_dash.state_selected.upper().replace('-', ' ')
            st.subheader(title)

            ###### User Statewide analysis
            ch_title = "Users Data Analysis"
            user_state_df = pp_dash.get_user_data_statewise(
                pp_dash.state_selected)

            user_state_fig = go.Figure(data=[
                go.Bar(name='AppOpenings %', y=user_state_df['APPOPENS'],
                       x=user_state_df['YEAR'],
                       marker={'color': 'lightgreen'}),
                go.Bar(name='Registered Users %',
                       y=user_state_df['REGISTEREDUSERS'],
                       x=user_state_df['YEAR'], marker={'color': 'orange'})
            ])
            user_state_fig.update_layout(barmode='group', title=ch_title)

            st.plotly_chart(user_state_fig)

            ###### User Brand Statewide analysis
            ch_title = "Brand wise User count for the year " + pp_dash.yr_selected
            user_brand_df = pp_dash.get_user_brand_statewise(
                pp_dash.state_selected, pp_dash.yr_selected)

            user_brand_fig = go.Figure(data=[
                go.Pie(labels=user_brand_df['BRAND'],
                       values=user_brand_df['COUNT'],
                       hole=.4, textinfo='label+percent',
                       texttemplate='%{label}<br>%{percent:1%f}'
                       , insidetextorientation='horizontal',
                       textfont=dict(color='#000000')
                       , marker_colors=px.colors.qualitative.Prism)])
            user_brand_fig.update_layout(title=ch_title)

            st.plotly_chart(user_brand_fig)

        with tab4:
            title = "Overall Analysis of the Transactions"
            st.subheader(title)
            ###### Year Transaction analysis
            ch_title = "Total Transactions in all States for " + pp_dash.mode_selected + " in the year " + pp_dash.yr_selected
            yr_count_df = pp_dash.get_yr_transaction(pp_dash.mode_selected,
                                                     pp_dash.yr_selected)

            yr_count_df['STATE'] = yr_count_df['STATE'].str.capitalize()

            yr_fig = px.bar(yr_count_df, x="STATE", y="COUNT",
                            hover_data=['COUNT'], title=ch_title)

            st.plotly_chart(yr_fig, theme="streamlit",
                            use_container_width=True)

            ###### Overall analysis
            ch_title = "Total Transaction Count over the Years"
            overall_df = pp_dash.get_overall_trans()
            overall_fig = px.pie(overall_df, names="YEAR", values="COUNT",
                                 color_discrete_sequence=px.colors.sequential.RdBu,
                                 hover_data=['COUNT'], title=ch_title)
            st.plotly_chart(overall_fig)


########## Calling the Main program
main()




class phonepe_data:
    def __init__(self, p_data_dir, p_data_type):
        self.data_dir = p_data_dir
        self.data_type = p_data_type

        self.tbl_map_user = 'map_user'
        self.tbl_agg_trans = 'agg_trans'
        self.tbl_agg_user = 'agg_user'
        self.tbl_top_trans = 'top_trans'
        self.tbl_top_user = 'top_user'
        self.tbl_map_trans = 'map_trans'

    # MySQL DB connect
    def mysql_db_connect(self):
        try:

            hostname = "localhost"
            database = "phonepe"
            username = "root"
            password = "sumit"

            self.engine = create_engine(
                "mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname,
                                                                 db=database,
                                                                 user=username,
                                                                 pw=password))
        except:
            print('Error mysql_db_connect - MYSQL DB connection failed!!')

    # Read the folders from the state folder
    def read_file_data(self):

        lv_data = []

        # iterate over files in
        # that directory

        yr_data = []
        prev_state = ''

        for root, dirs, files in os.walk(self.data_dir):
            quarter_data = []

            file_count = 0;
            for filename in files:

                # Open each files in the folder
                file_count = file_count + 1;
                q_data = json.load(open(root + '\\' + filename, "r"))

                matches = re.search(r"^(.+?)state\\(.+?)\\(([0-9]+))$", root)
                state = matches.groups()[1]

                # set the prev_state, if the previous state data is appended already or if this is the first state to retrieve the data
                if (prev_state == ''):
                    prev_state = state

                year = matches.groups()[2]
                q = filename.split('.')[0]
                q_data_details = []

                # if there is any error, iterate to next json file or quarter file
                try:
                    if self.data_type == 'agg_trans':

                        # get the transactiondata from the file
                        for items in enumerate(
                                q_data['data']['transactionData']):
                            detailed_data = {items[1]['name']: {
                                'count': items[1]['paymentInstruments'][0][
                                    'count'],
                                'amount': items[1]['paymentInstruments'][0][
                                    'amount']
                                }
                                             }
                            q_data_details.append(detailed_data)

                    elif self.data_type == 'agg_user':

                        # get the user data from the file
                        for items in enumerate(
                                q_data['data']['usersByDevice']):
                            detailed_data = {items[1]['brand']: {
                                'count': items[1].get('count', 0),
                                'percentage': items[1].get('percentage', 0)
                                }
                                             }
                            q_data_details.append(detailed_data)

                    elif self.data_type == 'top_trans':

                        # get the user data from the file
                        for items in enumerate(q_data['data']['districts']):
                            detailed_data = {items[1]['entityName']: {
                                'count': items[1]['metric']['count'],
                                'amount': items[1]['metric']['amount']
                                }
                                             }
                            q_data_details.append(detailed_data)

                    elif self.data_type == 'top_user':

                        # get the user data from the file
                        for items in enumerate(q_data['data']['districts']):
                            detailed_data = {items[1]['name']: {
                                'registeredUsers': items[1][
                                    'registeredUsers']}}
                            q_data_details.append(detailed_data)



                    elif self.data_type == 'map_trans':

                        # get the user data from the file
                        for items in enumerate(
                                q_data['data']['hoverDataList']):
                            lv_district_name = \
                            items[1]['name'].split(' district')[0]
                            detailed_data = {lv_district_name: {
                                'count': items[1]['metric'][0]['count'],
                                'amount': items[1]['metric'][0]['amount']
                                }
                                             }
                            q_data_details.append(detailed_data)

                    elif self.data_type == 'map_user':

                        # get the user data from the file

                        detailed_data = json.dumps(q_data['data']['hoverData'])

                        detailed_data = json.loads(
                            detailed_data.replace(' district', ''))
                        q_data_details.append(detailed_data)
                except:
                    next

                # append to the quarter
                if len(q_data_details):
                    quarter_data.append({q: q_data_details})

                # when all the files data are collected, append the data to the year
                if (file_count == len(files)):
                    yr_data.append({year: quarter_data})
                    quarter_data = []

                # when the current state data retreival for all years is complete, reset the state name and append the data with the state name
                if prev_state != state:
                    lv_data.append({prev_state: yr_data})
                    prev_state = ''
                    yr_data = []

        return lv_data

    # Store the DF to mysql DB
    def store_phone_data_to_db(self, pp_df):
        try:
            self.mysql_db_connect()

        except Exception as ex:
            print(ex)

        # convert the DF to sql and inserts to mysql DB
        try:
            if self.data_type == 'map_user':
                pp_df.to_sql(self.tbl_map_user, self.engine,
                             if_exists='replace', index=False)
                print('Map User Data stored to mysql DB')
            elif self.data_type == 'agg_trans':
                pp_df.to_sql(self.tbl_agg_trans, self.engine,
                             if_exists='replace', index=False)
                print('Aggregated Transaction Data stored to mysql DB')
            elif self.data_type == 'agg_user':
                pp_df.to_sql(self.tbl_agg_user, self.engine,
                             if_exists='replace', index=False)
                print('Aggregated User Data stored to mysql DB')
            elif self.data_type == 'top_trans':
                pp_df.to_sql(self.tbl_top_trans, self.engine,
                             if_exists='replace', index=False)
                print('Top Transaction Data stored to mysql DB')
            elif self.data_type == 'top_user':
                pp_df.to_sql(self.tbl_top_user, self.engine,
                             if_exists='replace', index=False)
                print('Top User Data stored to mysql DB')
            elif self.data_type == 'map_trans':
                pp_df.to_sql(self.tbl_map_trans, self.engine,
                             if_exists='replace', index=False)
                print('Map Transaction Data stored to mysql DB')


        except:
            print('Error storing the date to mysql for Data ', self.data_type)

    # convert the dict constructed to a DF
    def convert_dict_to_df(self, phone_data):
        df_data = []
        for st_data in phone_data:
            for name, data in st_data.items():
                for yr_data in data:
                    for yr, yr_details in yr_data.items():
                        for qtr_data in yr_details:
                            for qtr, qtr_details in qtr_data.items():

                                for dt in qtr_details:
                                    each_data = dict()
                                    for i in dt.keys():
                                        each_data['state'] = name
                                        each_data['year'] = yr
                                        each_data['quarter'] = qtr
                                        each_data['metric'] = i
                                        for j in dt[i].keys():
                                            each_data[j] = dt[i][j]

                                        df_data.append(each_data)
                                        each_data = {}

        return pd.DataFrame.from_dict(df_data)


# Create object for class phonepe_data for Aggregated Transaction jsons

pp_agg_trans_data_obj = phonepe_data(
    'D:pulse/data/aggregated/transaction/country/india/state',
    'agg_trans')
pp_agg_trans_data = pp_agg_trans_data_obj.read_file_data()  # read the data from file and format the data
pp_agg_trans_df = pp_agg_trans_data_obj.convert_dict_to_df(
    pp_agg_trans_data)  # convert the formatted data to DF
pp_agg_trans_data_obj.store_phone_data_to_db(
    pp_agg_trans_df)  # Store the DF to mysql

# Create object for class phonepe_data for Aggregated user jsons
pp_agg_user_data_obj = phonepe_data(
    'D:pulse/data/aggregated/user/country/india/state',
    'agg_user')
pp_agg_user_data = pp_agg_user_data_obj.read_file_data()  # read the data from file and format the data
pp_agg_user_df = pp_agg_user_data_obj.convert_dict_to_df(
    pp_agg_user_data)  # convert the formatted data to DF
pp_agg_user_data_obj.store_phone_data_to_db(
    pp_agg_user_df)  # Store the DF to mysql

# Create object for class phonepe_data for Top transaction jsons
pp_top_trans_data_obj = phonepe_data(
    'D:pulse/data/top/transaction/country/india/state',
    'top_trans')
pp_top_trans_data = pp_top_trans_data_obj.read_file_data()  # read the data from file and format the data
pp_top_trans_df = pp_top_trans_data_obj.convert_dict_to_df(
    pp_top_trans_data)  # convert the formatted data to DF
pp_top_trans_data_obj.store_phone_data_to_db(
    pp_top_trans_df)  # Store the DF to mysql

# Create object for class phonepe_data for Top user jsons
pp_top_user_data_obj = phonepe_data(
    'D:pulse/data/top/user/country/india/state',
    'top_user')
pp_top_user_data = pp_top_user_data_obj.read_file_data()  # read the data from file and format the data
pp_top_user_df = pp_top_user_data_obj.convert_dict_to_df(
    pp_top_user_data)  # convert the formatted data to DF
pp_top_user_data_obj.store_phone_data_to_db(
    pp_top_user_df)  # Store the DF to mysql

# Create object for class phonepe_data for map transaction jsons
pp_map_trans_data_obj = phonepe_data(
    'D:pulse/data/map/transaction/hover/country/india/state',
    'map_trans')
pp_map_trans_data = pp_map_trans_data_obj.read_file_data()  # read the data from file and format the data
pp_map_trans_df = pp_map_trans_data_obj.convert_dict_to_df(
    pp_map_trans_data)  # convert the formatted data to DF
pp_map_trans_data_obj.store_phone_data_to_db(
    pp_map_trans_df)  # Store the DF to mysql

# Create object for class phonepe_data for map user jsons
pp_map_user_data_obj = phonepe_data(
    'D:pulse/data/map/user/hover/country/india/state',
    'map_user')
pp_map_user_data = pp_map_user_data_obj.read_file_data()  # read the data from file and format the data
pp_map_user_df = pp_map_user_data_obj.convert_dict_to_df(
    pp_map_user_data)  # convert the formatted data to DF
pp_map_user_data_obj.store_phone_data_to_db(
    pp_map_user_df)  # Store the DF to mysql
