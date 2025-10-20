-- SQLBench-DS query 40 derived from TPC-DS query 40 under the terms of the TPC Fair Use Policy.
-- TPC-DS queries are Copyright 2021 Transaction Processing Performance Council.
-- This query was generated at scale factor 1.
select  
   w_state
  ,i_item_id
  ,sum(case when (cast(d_date as date) < cast ('2002-05-18' as date)) 
 		then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_before
  ,sum(case when (cast(d_date as date) >= cast ('2002-05-18' as date)) 
 		then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_after
 from
   catalog_sales left outer join catalog_returns on
       (cs_order_number = cr_order_number 
        and cs_item_sk = cr_item_sk)
  ,warehouse 
  ,item
  ,date_dim
 where
     i_current_price between 0.99 and 1.49
 and i_item_sk          = cs_item_sk
 and cs_warehouse_sk    = w_warehouse_sk 
 and cs_sold_date_sk    = d_date_sk
 and d_date between (cast ('2002-05-18' as date) - INTERVAL '30 DAYS')
                and (cast ('2002-05-18' as date) + INTERVAL '30 DAYS') 
 group by
    w_state,i_item_id
 order by w_state,i_item_id
 LIMIT 100;

