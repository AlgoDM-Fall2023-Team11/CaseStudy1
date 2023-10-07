import streamlit as st
import pandas as pd
import snowflake.connector
from datetime import datetime
from queries import query90, query91, query92, query93, query94, query95, query96

# Replace with your Snowflake credentials
snowflake_credentials = {
    'account': 'uyrevwi-yib28968',
    'user': 'usashi',
    'password': 'Way2Boston$12345',
    'warehouse': 'COMPUTE_WH',
    'database': 'SNOWFLAKE_SAMPLE_DATA',
    'schema': 'TPCDS_SF10TCL',
    'role': 'ACCOUNTADMIN',
}

# Create a Snowflake connection
snowflake_connection = snowflake.connector.connect(**snowflake_credentials)

# Dictionary containing query definitions
query_definitions = {
    0: "Query 0 Definition: ...",
    1: "Query 1 Definition: ...",
    2: "Query 2 Definition: ...",
    3: "Query 3 Definition: ...",
    4: "Query 4 Definition: ...",
    5: "Query 5 Definition: ...",
    6: "Query 6 Definition: ...",
    7: "Query 7 Definition: ...",
    8: "Query 8 Definition: ...",
    9: "Query 9 Definition: ..."
}

# Dictionary containing actual queries with query substitution parameters
actual_queries = {
    0: query90,

    1: query91,
    
    2: query92,
    
    3: query93,
    
    4: query94,

    5: query95,

    6: query96,

    7: """Query 7 SQL""",

    8: """Query 8 SQL""",

    9: """Query 9 SQL"""
}
formatted_ws_date = None
formatted_date = None
# Define a function to execute the query without caching
def execute_query_snowflake(query):
    try:
        cursor = snowflake_connection.cursor()
        cursor.execute(query)
        df = pd.DataFrame.from_records(iter(cursor), columns=[x[0] for x in cursor.description])
        return df
    except Exception as e:
        st.error(f"Error executing the query: {str(e)}")
        st.error(f"Query: {query}")
        raise e
    finally:
        if snowflake_connection:
            snowflake_connection.close()

# Streamlit app
st.title("TPC-DS Query Selector")

# Select a query
selected_query = st.selectbox("Select Query", list(query_definitions.keys()))

# Display the selected query definition
st.subheader("Query Definition:")
st.write(query_definitions[selected_query])

# Input query substitution parameters (if applicable)
if selected_query in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]:
    st.subheader("Query Substitution Parameters:")
    # Define the parameters for each query
    year = month = ws_date = buy_potential = gmt_offset = manufacturer_id = reason_number = dependant_count = date = state = hours = hoursam = hourspm =  None
    # Define the parameters for each query
    if selected_query == 0:
        # Input query substitution parameters for Query 6
        dependant_count = st.number_input("dependant Count", min_value=1, max_value=100, value=6)
        hoursam = st.number_input("Morning", min_value=1, max_value=24, value=8)   
        hourspm = st.number_input("Evening", min_value=1, max_value=24, value=19)
    elif selected_query == 1:
        # Input query substitution parameters for Query 1
        year = st.number_input("Year", min_value=1980, max_value=2023, value=1998)
        month = st.number_input("Month", min_value=1, max_value=12, value=11)
        buy_potential = st.text_input("Buy Potential", "Unknown")
        gmt_offset = st.number_input("GMT Offset", min_value=-12, max_value=12, value=-7)
    elif selected_query == 2:
        # Input query substitution parameters for Query 2
        manufacturer_id = st.number_input("Manufacturer ID", min_value=1, max_value=1000, value=939)
        ws_date = st.date_input("Web Sales Date", pd.to_datetime('2000-01-27'))
        formatted_ws_date = ws_date.strftime('%Y-%m-%d')
    elif selected_query == 3:
        # Input query substitution parameters for Query 3
        reason_number = st.number_input("Reason Number", min_value=1, max_value=100, value=28)
    elif selected_query == 4:
        # Input query substitution parameters for Query 4
        date = st.number_input("Month",pd.to_datetime('1999-02-27'))
        state = st.text_input('State', 'GA')
    elif selected_query == 5:
        # Input query substitution parameters for Query 4
        date = st.date_input("Date", pd.to_datetime('1999-02-27'))
        formatted_date = date.strftime('%Y-%m-%d')
        state = st.text_input('State', 'GA')
    elif selected_query == 6:
        # Input query substitution parameters for Query 6
        dependant_count = st.number_input("dependant Count", min_value=1, max_value=100, value=7)
        hours = st.number_input("Hours", min_value=1, max_value=24, value=20)
    # Define parameters for other queries (3 to 9) in a similar way

# Execute the query
if st.button("Run Query"):
    # Build the query with substitution parameters
    query = actual_queries[selected_query]
    if selected_query in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]:
        query = query.format(
            #query90
            hoursam = hoursam,
            hourspm = hourspm,
            dependant_count = dependant_count,

            #query 91
            year=year,
            month=month,
            buy_potential=buy_potential,
            gmt_offset=gmt_offset,

            #query 92
            manufacturer_id = manufacturer_id,
            formatted_ws_date = formatted_ws_date,

            #query 93
            reason_number = reason_number,

            #query94
            formatted_date = formatted_date,
            state = state,

            # #query95
            # date = date,
            # state = state,

            # #query96
            # dependant_count = dependant_count,
            hours = hours

        )
        
    # Run the query and display results
    st.subheader("Query Results:")
    result_df = execute_query_snowflake(query)
    st.write(result_df)
