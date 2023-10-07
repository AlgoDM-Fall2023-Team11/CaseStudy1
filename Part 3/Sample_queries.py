# Dictionary containing actual queries with query substitution parameters
actual_queries = {
    0: """
        select cast(amc as decimal(15,4))/cast(pmc as decimal(15,4)) am_pm_ratio
        from (
            select count(*) amc
            from web_sales, household_demographics, time_dim, web_page
            where ws_sold_time_sk = time_dim.t_time_sk
                and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
                and ws_web_page_sk = web_page.wp_web_page_sk
                and time_dim.t_hour between {hoursam} and {hoursam}+1
                and household_demographics.hd_dep_count = {dependant_count}
                and web_page.wp_char_count between 5000 and 5200
        ) at,
        (
            select count(*) pmc
            from web_sales, household_demographics, time_dim, web_page
            where ws_sold_time_sk = time_dim.t_time_sk
                and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
                and ws_web_page_sk = web_page.wp_web_page_sk
                and time_dim.t_hour between {hourspm} and {hourspm}+1
                and household_demographics.hd_dep_count = {dependant_count}
                and web_page.wp_char_count between 5000 and 5200
        ) pt
        order by am_pm_ratio
        limit 100;
    """,

    1: """
        SELECT  
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
            AND (
                (cd_marital_status = 'M' AND cd_education_status = 'Unknown')
                OR (cd_marital_status = 'W' AND cd_education_status = 'Advanced Degree')
            )
            AND hd_buy_potential LIKE '{buy_potential}%'
            AND ca_gmt_offset = {gmt_offset}
        GROUP BY cc_call_center_id, cc_name, cc_manager, cd_marital_status, cd_education_status
        ORDER BY sum(cr_net_loss) DESC;
    """,

    2: """
        SELECT  
            sum(ws_ext_discount_amt) AS "Excess Discount Amount" 
        FROM 
            web_sales, item, date_dim
        WHERE
            i_manufact_id = {manufacturer_id}
            AND i_item_sk = ws_item_sk 
            AND d_date BETWEEN '{formatted_ws_date}' AND dateadd(day, 90, to_date('{formatted_ws_date}'))
            AND d_date_sk = ws_sold_date_sk 
            AND ws_ext_discount_amt > (
                SELECT 1.3 * avg(ws_ext_discount_amt) 
                FROM web_sales, date_dim
                WHERE ws_item_sk = i_item_sk 
                AND d_date BETWEEN '{formatted_ws_date}' AND dateadd(day, 90, to_date('{formatted_ws_date}'))
                AND d_date_sk = ws_sold_date_sk 
            ) 
        ORDER BY sum(ws_ext_discount_amt)
        LIMIT 100;
    """,

    3: """
        select  ss_customer_sk,
                sum(act_sales) sumsales
        from (select ss_item_sk,
                    ss_ticket_number,
                    ss_customer_sk,
                    case when sr_return_quantity is not null then (ss_quantity-sr_return_quantity)*ss_sales_price
                                                            else (ss_quantity*ss_sales_price) end act_sales
                from store_sales left outer join store_returns on (sr_item_sk = ss_item_sk
                                                               and sr_ticket_number = ss_ticket_number),
                     reason
                where sr_reason_sk = r_reason_sk
                  and r_reason_desc = 'reason {reason_number}') t
        group by ss_customer_sk
        order by sumsales, ss_customer_sk
        limit 100;
    """,

    4: """
        SELECT  
            count(distinct ws_order_number) as "order count",
            sum(ws_ext_ship_cost) as "total shipping cost",
            sum(ws_net_profit) as "total net profit"
        FROM
            web_sales ws1
            ,date_dim
            ,customer_address
            ,web_site
        WHERE
            d_date BETWEEN '{formatted_date}' AND dateadd(day, 60, to_date('{formatted_date}'))
            AND ws1.ws_ship_date_sk = d_date_sk
            AND ws1.ws_ship_addr_sk = ca_address_sk
            AND ca_state = '{state}'
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
        LIMIT 100;
    """,

    5: """
        with ws_wh as (
            select ws1.ws_order_number, ws1.ws_warehouse_sk wh1, ws2.ws_warehouse_sk wh2
            from web_sales ws1, web_sales ws2
            where ws1.ws_order_number = ws2.ws_order_number
                and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk
        )
        select  
            count(distinct ws_order_number) as "order count",
            sum(ws_ext_ship_cost) as "total shipping cost",
            sum(ws_net_profit) as "total net profit"
        from
            web_sales ws1
            ,date_dim
            ,customer_address
            ,web_site
        WHERE
            d_date BETWEEN '{formatted_date}' and 
            dateadd(day, 60, to_date('{formatted_date}'))
            AND ws1.ws_ship_date_sk = d_date_sk
            AND ws1.ws_ship_addr_sk = ca_address_sk
            AND ca_state = '{state}'
            AND ws1.ws_web_site_sk = web_site_sk
            AND web_company_name = 'pri'
            AND ws1.ws_order_number in (select ws_order_number
                                        from ws_wh)
            AND ws1.ws_order_number in (select wr_order_number
                                        from web_returns, ws_wh
                                        where wr_order_number = ws_wh.ws_order_number)
        ORDER BY count(distinct ws_order_number)
        LIMIT 100;
    """,

    6: """
        select  count(*) 
        from store_sales
            ,household_demographics 
            ,time_dim, store
        where ss_sold_time_sk = time_dim.t_time_sk   
            and ss_hdemo_sk = household_demographics.hd_demo_sk 
            and ss_store_sk = s_store_sk
            and time_dim.t_hour = {hours}
            and time_dim.t_minute <= 30
            and household_demographics.hd_dep_count = {dependant_count}
            and store.s_store_name = 'ese'
        order by count(*)
        limit 100;
    """,

    7: """with ssci as (
select ss_customer_sk customer_sk
      ,ss_item_sk item_sk
from store_sales,date_dim
where ss_sold_date_sk = d_date_sk
  and d_month_seq between {dms} and {dms} + 11
group by ss_customer_sk
        ,ss_item_sk),
csci as(
 select cs_bill_customer_sk customer_sk
      ,cs_item_sk item_sk
from catalog_sales,date_dim
where cs_sold_date_sk = d_date_sk
  and d_month_seq between 1200 and 1200 + 11
group by cs_bill_customer_sk
        ,cs_item_sk)
 select  sum(case when ssci.customer_sk is not null and csci.customer_sk is null then 1 else 0 end) store_only
      ,sum(case when ssci.customer_sk is null and csci.customer_sk is not null then 1 else 0 end) catalog_only
      ,sum(case when ssci.customer_sk is not null and csci.customer_sk is not null then 1 else 0 end) store_and_catalog
from ssci full outer join csci on (ssci.customer_sk=csci.customer_sk
                               and ssci.item_sk = csci.item_sk)
limit 100;""",

    8: """
    select i_item_desc
      ,i_category
      ,i_class
      ,i_current_price
      ,sum(ss_ext_sales_price) as itemrevenue
      ,sum(ss_ext_sales_price)*100/sum(sum(ss_ext_sales_price)) over
          (partition by i_class) as revenueratio
from
	store_sales
    	,item
    	,date_dim
where
	ss_item_sk = i_item_sk
  	and i_category in ('{category1}', '{category2}', '{category3}')
  	and ss_sold_date_sk = d_date_sk
	and d_date between cast('1999-02-22' as date)
				and (cast('1999-02-22' as date) + interval '30 days')
group by
	i_item_id
        ,i_item_desc
        ,i_category
        ,i_class
        ,i_current_price
order by
	i_category
        ,i_class
        ,i_item_id
        ,i_item_desc
        ,revenueratio; """
        ,
    
    9: """
    select 
   substr(w_warehouse_name,1,20)
  ,sm_type
  ,cc_name
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk <= 30 ) then 1 else 0 end)  as "30 days"
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 30) and
                 (cs_ship_date_sk - cs_sold_date_sk <= 60) then 1 else 0 end )  as "31-60 days"
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 60) and
                 (cs_ship_date_sk - cs_sold_date_sk <= 90) then 1 else 0 end)  as "61-90 days"
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 90) and
                 (cs_ship_date_sk - cs_sold_date_sk <= 120) then 1 else 0 end)  as "91-120 days"
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk  > 120) then 1 else 0 end)  as ">120 days"
from
   catalog_sales
  ,warehouse
  ,ship_mode
  ,call_center
  ,date_dim
where
    d_month_seq between {DMS} and {DMS} + 11
and cs_ship_date_sk   = d_date_sk
and cs_warehouse_sk   = w_warehouse_sk
and cs_ship_mode_sk   = sm_ship_mode_sk
and cs_call_center_sk = cc_call_center_sk
group by
   substr(w_warehouse_name,1,20)
  ,sm_type
  ,cc_name
order by substr(w_warehouse_name,1,20)
        ,sm_type
        ,cc_name
limit 100;"""

}
