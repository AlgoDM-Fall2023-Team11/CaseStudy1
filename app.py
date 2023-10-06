import streamlit as st
import pandas as pd
import snowflake.connector

# Replace with your Snowflake credentials
snowflake_credentials = {
    'account': 'zaplmed-dqb37133',
    'user': 'NAKULSHILEDAR',
    'password': '13April1998$',
    'warehouse': 'Analytics_WH',
    'database': 'SNOWFLAKE_SAMPLE_DATA',
    'schema': 'TPCDS_SF10TCL',
    'role': 'ACCOUNTADMIN',
}

# Create a Snowflake connection
snowflake_connection = snowflake.connector.connect(**snowflake_credentials)

# Dictionary containing query definitions
query_definitions = {
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
    1: """SELECT  
        cc_call_center_id Call_Center,
        cc_name Call_Center_Name,
        cc_manager Manager,
        sum(cr_net_loss) Returns_Loss
    FROM
        call_center,
        catalog_returns,
        date_dim,
        customer,
        customer_address,
        customer_demographics,
        household_demographics
    WHERE
        cr_call_center_sk = cc_call_center_sk
        AND cr_returned_date_sk = d_date_sk
        AND cr_returning_customer_sk = c_customer_sk
        AND cd_demo_sk = c_current_cdemo_sk
        AND hd_demo_sk = c_current_hdemo_sk
        AND ca_address_sk = c_current_addr_sk
        AND d_year = {year}
        AND d_moy = {month}
        AND ((cd_marital_status = 'M' AND cd_education_status = 'Unknown')
        OR (cd_marital_status = 'W' AND cd_education_status = 'Advanced Degree'))
        AND hd_buy_potential LIKE 'Unknown%'
        AND ca_gmt_offset = {gmt_offset}
    GROUP BY cc_call_center_id, cc_name, cc_manager, cd_marital_status, cd_education_status
    ORDER BY sum(cr_net_loss) DESC;""",
    
    2: """SELECT  
        sum(ws_ext_discount_amt) AS "Excess Discount Amount" 
    FROM 
        web_sales, item, date_dim
    WHERE
        i_manufact_id = {manufacturer_id}
        AND i_item_sk = ws_item_sk 
        AND d_date BETWEEN '2002-02-16' AND dateadd(day, 90, to_date('2002-02-16'))
        AND d_date_sk = ws_sold_date_sk 
        AND ws_ext_discount_amt > (
            SELECT 1.3 * avg(ws_ext_discount_amt) 
            FROM web_sales, date_dim
            WHERE ws_item_sk = i_item_sk 
            AND d_date BETWEEN '2002-02-16' AND dateadd(day, 90, to_date('2002-02-16'))
            AND d_date_sk = ws_sold_date_sk 
        ) 
    ORDER BY sum(ws_ext_discount_amt)
    LIMIT 100;""",
    
    3: """SELECT  
        ss_customer_sk,
        sum(act_sales) sumsales
    FROM (
        SELECT 
            ss_item_sk,
            ss_ticket_number,
            ss_customer_sk,
            CASE 
                WHEN sr_return_quantity IS NOT NULL THEN (ss_quantity - sr_return_quantity) * ss_sales_price
                ELSE (ss_quantity * ss_sales_price)
            END AS act_sales
        FROM 
            store_sales 
        LEFT OUTER JOIN store_returns ON (sr_item_sk = ss_item_sk AND sr_ticket_number = ss_ticket_number)
        WHERE sr_reason_sk = r_reason_sk AND r_reason_desc = 'reason 70'
        ) t
    GROUP BY ss_customer_sk
    ORDER BY sumsales, ss_customer_sk
    LIMIT 100;""",
    
    4: """SELECT  
        count(distinct ws_order_number) as "order count",
        sum(ws_ext_ship_cost) as "total shipping cost",
        sum(ws_net_profit) as "total net profit"
    FROM
        web_sales ws1
        ,date_dim
        ,customer_address
        ,web_site
    WHERE
        d_date BETWEEN '2002-2-01' AND dateadd(day, 60, to_date('2002-2-01'))
        AND ws1.ws_ship_date_sk = d_date_sk
        AND ws1.ws_ship_addr_sk = ca_address_sk
        AND ca_state = 'CA'
        AND ws1.ws_web_site_sk = web_site_sk
        AND web_company_name = 'pri'
        AND EXISTS (
            SELECT *
            FROM web_sales ws2
            WHERE ws1.ws_order_number = ws2.ws_order_number
              AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk
        )
        AND NOT EXISTS (
            SELECT *
            FROM web_returns wr1
            WHERE ws1.ws_order_number = wr1.wr_order_number
        )
    ORDER BY count(distinct ws_order_number)
    LIMIT 100;""",
    5: """Query 5 SQL""",
    6: """Query 6 SQL""",
    7: """Query 7 SQL""",
    8: """Query 8 SQL""",
    9: """Query 9 SQL"""
}

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
if selected_query in [1, 2, 3, 4, 5, 6, 7, 8, 9]:
    st.subheader("Query Substitution Parameters:")
    # Define the parameters for each query
    year = month = marital_status = education_status = buy_potential = gmt_offset = manufacturer_id = start_date = end_date = discount_threshold = None
    # Define the parameters for each query
    if selected_query == 1:
        # Input query substitution parameters for Query 1
        year = st.number_input("Year", min_value=2000, max_value=2023, value=2002)
        month = st.number_input("Month", min_value=1, max_value=12, value=11)
        marital_status = st.selectbox("Marital Status", ["M", "W"])
        education_status = st.selectbox("Education Status", ["Unknown", "Advanced Degree"])
        buy_potential = st.text_input("Buy Potential", "Unknown%")
        gmt_offset = st.number_input("GMT Offset", min_value=-12, max_value=12, value=-6)
    elif selected_query == 2:
        # Input query substitution parameters for Query 2
        manufacturer_id = st.number_input("Manufacturer ID", min_value=1, max_value=1000, value=939)
        start_date = st.date_input("Start Date", pd.to_datetime('2001-01-27'))
        end_date = st.date_input("End Date", pd.to_datetime('2001-04-17'))
        discount_threshold = st.number_input("Discount Threshold (%)", min_value=0, max_value=100, value=30)
    # Define parameters for other queries (3 to 9) in a similar way

# Execute the query
if st.button("Run Query"):
    # Build the query with substitution parameters
    query = actual_queries[selected_query]
    if selected_query in [1, 2, 3, 4, 5, 6, 7, 8, 9]:
        query = query.format(
            year=year,
            month=month,
            marital_status=marital_status,
            education_status=education_status,
            buy_potential=buy_potential,
            gmt_offset=gmt_offset,
            manufacturer_id=manufacturer_id,
            discount_threshold=discount_threshold
        )

    # Run the query and display results
    st.subheader("Query Results:")
    result_df = execute_query_snowflake(query)
    st.write(result_df)
