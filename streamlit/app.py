import streamlit as st
import pandas as pd
import snowflake.connector
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
from Sample_queries import actual_queries

# Replace with your Snowflake credentials
snowflake_credentials = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA'),
    'role': os.getenv('SNOWFLAKE_ROLE'),
}
# Create a Snowflake connection
snowflake_connection = snowflake.connector.connect(**snowflake_credentials)

# Dictionary containing query definitions
query_definitions = {
    0: "What is the ratio between the number of items sold over the internet in the morning (8 to 9 am) to the number of items sold in the evening (7 to 8 pm) of customers with a specified number of dependents? Consider only websites with a high amount of content.",
    1: "Display total returns of catalog sales by call center and manager in a particular month for male customers of unknown education or female customers with advanced degrees with a specified buy potential and from a particular time zone.",
    2: "Compute the total discount on web sales of items from a given manufacturer over a particular 90 day period for sales whose discount exceeded 30 percent over the average discount of items from that manufacturer in that period of time.",
    3: "For a given merchandise return reason, report on customer's total cost of purchases minus the cost of returned items.",
    4: "Produce a count of web sales and total shipping cost and net profit in a given 60 day period to customers in a given state from a named web site for non-returned orders shipped from more than one warehouse.",
    5: "Produce a count of web sales and total shipping cost and net profit in a given 60 day period to customers in a given state from a named web site for returned orders shipped from more than one warehouse.",
    6: "Compute a count of sales from a named store to customers with a given number of dependents made in a specified half-hour period of the day.",
    7: "Generate counts of promotional sales and total sales, and their ratio from the web channel for a particular item category and month to customers in a given time zone.",
    8: "Report on items sold in a given 30 day period, belonging to the specified category.",
    9: "For catalog sales, create a report showing the counts of orders shipped within 30 days, from 31 to 60 days, from 61 to 90 days, from 91 to 120 days, and over 120 days within a given year, grouped by warehouse, call center, and shipping mode."
}


# Dictionary containing actual queries with query substitution parameters

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
    year = month = ws_date = buy_potential = gmt_offset = manufacturer_id = reason_number = dependant_count = date = state = hours = hoursam = hourspm  = DMS = category1 = category2 = category3 = None
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
    elif selected_query == 7:
        # Input query substitution parameters for Query 7
        DMS = st.number_input("d_month_seq", min_value=1, max_value=1500, value=1200)
    elif selected_query == 8:
        # Input query substitution parameters for Query 8
        date = st.date_input("Date", pd.to_datetime('1999-02-22'))
        category1 = st.text_input('category1','Sports')
        category2 = st.text_input('category2','Books')
        category3 = st.text_input('category3','Home')
    elif selected_query == 9:
        # Input query substitution parameters for Query 9
        DMS = st.number_input("d_month_seq", min_value=1, max_value=1500, value=1200)

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

            #query94 and query95
            formatted_date = formatted_date,
            state = state,

            # #query96
            hours = hours,

            #query97
            DMS= DMS,

            #query98
            category1= category1,
            category2= category2,
            category3= category3


        )
        
    # Run the query and display results
    st.subheader("Query Results:")
    result_df = execute_query_snowflake(query)
    st.write(result_df)
