query91 = """SELECT  
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
        AND hd_buy_potential LIKE '{buy_potential}%'
        AND ca_gmt_offset = {gmt_offset}
    GROUP BY cc_call_center_id, cc_name, cc_manager, cd_marital_status, cd_education_status
    ORDER BY sum(cr_net_loss) DESC;"""


query92 = """SELECT  
        sum(ws_ext_discount_amt) AS "Excess Discount Amount" 
    FROM 
        web_sales, item, date_dim
    WHERE
        i_manufact_id = {manufacturer_id}
        AND i_item_sk = ws_item_sk 
        AND d_date BETWEEN {ws_date} AND dateadd(day, 90, to_date({ws_date}))
        AND d_date_sk = ws_sold_date_sk 
        AND ws_ext_discount_amt > (
            SELECT 1.3 * avg(ws_ext_discount_amt) 
            FROM web_sales, date_dim
            WHERE ws_item_sk = i_item_sk 
            AND d_date BETWEEN {ws_date} AND dateadd(day, 90, to_date({ws_date}))
            AND d_date_sk = ws_sold_date_sk 
        ) 
    ORDER BY sum(ws_ext_discount_amt)
    LIMIT 100;"""


query93 = """SELECT  
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
        WHERE sr_reason_sk = r_reason_sk AND r_reason_desc = 'reason {reason_number}'
        ) t
    GROUP BY ss_customer_sk
    ORDER BY sumsales, ss_customer_sk
    LIMIT 100;"""

query94 = """SELECT  
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
    LIMIT 100;"""