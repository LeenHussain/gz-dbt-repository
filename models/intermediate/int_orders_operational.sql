SELECT
  o.orders_id,
  o.date_date,
  ROUND(o.margin + s.shipping_fee - (s.logcost + cast(s.ship_cost as float64)), 2) AS operational_margin,
  o.quantity,
  o.revenue,
  o.purchase_cost,
  o.margin,
  s.shipping_fee,
  s.logcost,
  s.ship_cost
FROM {{ ref('int_orders_margin') }} o
LEFT JOIN {{ ref('stg_raw__ship') }} s 
  USING (orders_id)
ORDER BY orders_id DESC










models
intermediate
int_orders_operational.sql

Save
897101112131415654231
  ,s.ship_cost
FROM {{ref("int_orders_margin")}} o
LEFT JOIN {{ref("stg_raw__ship")}} s 
  USING(orders_id)
ORDER BY orders_id desc
DAG showing model.my_new_project.int_orders_operational and 4 other nodes


Enter DAG selector
2+int_orders_operational+2
Update Graph

Enter dbt command
dbt run

} s 
  USING(orders_id)
ORDER BY orders_id desc