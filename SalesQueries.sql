-- Databricks notebook source
use sales_db

-- COMMAND ----------

-- MAGIC %md ## Data Preparation and Clensing

-- COMMAND ----------

select * from sales_raw limit 5

-- COMMAND ----------

-- MAGIC %md ### Remove null rows and bad records

-- COMMAND ----------

select * from sales_raw where orderid is null limit 5

-- COMMAND ----------

select * from sales_db.sales_raw where OrderId = "Order ID" limit 10

-- COMMAND ----------

select * from sales_db.sales_raw where orderid != "Order ID" and orderid is not null

-- COMMAND ----------

with temp_sales_raw as 
  (
    select * from sales_db.sales_raw where orderid != "Order ID" and orderid is not null
  )
select * from temp_sales_raw limit 10

-- COMMAND ----------

with temp_sales_raw as 
    (
      select * from sales_db.sales_raw where orderid != "Order ID" and orderid is not null
    )
  select * from temp_sales_raw 
  where orderid is null or orderid = "Order ID"
  limit 10

-- COMMAND ----------

select split(purchaseaddress, ",") as city 
from sales_db.sales_raw 
where orderid is not null and orderid != "Order ID"
limit 5

-- COMMAND ----------

select split(purchaseaddress, ",")[1] as city 
from sales_db.sales_raw 
where orderid is not null and orderid != "Order ID"
limit 5

-- COMMAND ----------

select split(split(purchaseaddress, ",")[2], " ")[1] as state 
from sales_db.sales_raw 
where orderid is not null and orderid != "Order ID"
limit 5

-- COMMAND ----------

select split(purchaseaddress, ",")[1] as city,
       split(split(purchaseaddress, ",")[2], " ")[1] as state 
from sales_db.sales_raw 
where orderid is not null and orderid != "Order ID"
limit 5

-- COMMAND ----------

select cast(OrderID as int) as OrderId,
      Product,
      QuantityOrdered,
      PriceEach,
      to_timestamp(OrderDate, 'MM/dd/yy HH:mm') as OrderDate,
      PurchaseAddress,
      split(purchaseaddress, ",")[1] as city,
      split(split(purchaseaddress, ",")[2], " ")[1] as state,
      year(to_timestamp(OrderDate, 'MM/dd/yy HH:mm')) as ReportYear,
      month(to_timestamp(OrderDate, 'MM/dd/yy HH:mm')) as Month
from sales_raw
where orderid is not null
  and orderid != "Order ID"

-- COMMAND ----------


