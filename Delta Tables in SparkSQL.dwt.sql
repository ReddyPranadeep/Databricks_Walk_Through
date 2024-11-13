-- Databricks notebook source
drop database if exists retail_db cascade

-- COMMAND ----------

SET spark.sql.warehouse.dir

-- COMMAND ----------

SET spark.sql.warehouse.dir

-- COMMAND ----------

-- %fs ls dbfs:/user/hive/warehouse 
-- This is the default location for the warehouse

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS retail_db;

-- COMMAND ----------

USE retail_db;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS retail_db.orders (
  order_id BIGINT
  , order_date DATE 
  -- WHEN WE TRY TO COPY JSON FILES INTO THIS TABLE, WE GET AN ERROR IF DATATYPE OF ORDER_DATE IS DATE SO JUST MENTIONING IT AS A STRING.
  , order_customer_id BIGINT
  , order_status STRING
) USING DELTA

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/public/retail_db_json/orders

-- COMMAND ----------

-- MAGIC %fs head dbfs:/public/retail_db_json/orders/part-00000

-- COMMAND ----------

SELECT * FROM JSON.`dbfs:/public/retail_db_json/orders/part-00000`

-- COMMAND ----------

COPY INTO orders
FROM 'dbfs:/public/retail_db_json/orders'
FILEFORMAT = JSON
