-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS retail_db

-- COMMAND ----------

use retail_db

-- COMMAND ----------

-- MAGIC
-- MAGIC %fs ls dbfs:/public/retail_db/

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

select current_date()

-- COMMAND ----------

-- MAGIC
-- MAGIC %fs ls dbfs:/public/retail_db/orders/

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW orders(
    order_id INT
    ,order_date STRING
    , customer_id INT
    , order_status STRING
) USING csv
OPTIONS(
  path='dbfs:/public/retail_db/orders/'
)

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

DESCRIBE orders

-- COMMAND ----------

-- %sql
-- CREATE OR REPLACE VIEW order_items(
--     order_item_id INT
--     , order_item_order_id INT
--     , order_item_product_id INT
--     , order_item_quantity INT
--     , order_item_subtotal FLOAT
--     -- , order_item_product_price FLOAT
-- ) USING csv
-- options(path='dbfs:/public/retail_db/order_items/')

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW order_items(
    order_item_id INT
    , order_item_order_id INT
    , order_item_product_id INT
    , order_item_quantity INT
    , order_item_subtotal FLOAT
    , order_item_product_price FLOAT
) USING csv
OPTIONS(path='dbfs:/public/retail_db/order_items/')

-- COMMAND ----------

describe orders

-- COMMAND ----------

describe order_items

-- COMMAND ----------

SELECT o.order_date,
  oi.order_item_product_id,
  round(sum(oi.order_item_subtotal), 2) as revenue
FROM orders AS o
  JOIN order_items AS oi
    ON o.order_id = oi.order_item_order_id
WHERE o.order_status IN ('COMPLETE' , 'CLOSED')
GROUP BY 1,2
ORDER BY 1,3 DESC

-- COMMAND ----------

select o.order_date, oi.order_item_product_id, o.order_status,oi.order_item_subtotal
from orders as o join order_items as oi 
on o.order_id = oi.order_item_order_id
WHERE order_item_product_id = 1004 and o.order_status IN ('COMPLETE', 'CLOSED')

-- COMMAND ----------

INSERT OVERWRITE DIRECTORY 'dbfs:/public/retail_db/daily_product_revenue'
USING PARQUET
SELECT o.order_date,
  oi.order_item_product_id,
  round(sum(oi.order_item_subtotal), 2) as revenue
FROM orders AS o
  JOIN order_items AS oi
    ON o.order_id = oi.order_item_order_id
WHERE o.order_status IN ('COMPLETE' , 'CLOSED')
GROUP BY 1,2
ORDER BY 1,3 DESC

-- COMMAND ----------

SELECT * FROM parquet.`/public/retail_db/daily_product_revenue`
ORDER BY order_date,revenue DESC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC help(spark.read.text)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC schemas_text = spark.read.text('dbfs:/public/retail_db/schemas.json', wholetext=True).first().value

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import json

-- COMMAND ----------

-- MAGIC %python
-- MAGIC column_details = json.loads(schemas_text)['orders']

-- COMMAND ----------

-- MAGIC %python
-- MAGIC columns = sorted(column_details, key=lambda col: col['column_position'])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # spark.read.csv('dbfs:/public/retail_db/orders', inferSchema=True).toDF('order_id', 'order_date', 'order_customer_id', 'order_status').show()
-- MAGIC # spark.read.csv('dbfs:/public/retail_db/orders', inferSchema=True).toDF(*columns).show()
-- MAGIC orders = spark.read.csv('dbfs:/public/retail_db/orders', inferSchema=True).toDF('order_id', 'order_date', 'order_customer_id', 'order_status')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC orders.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import count,col
-- MAGIC # importing these functions:col to sort
-- MAGIC orders. \
-- MAGIC   groupBy('order_status').\
-- MAGIC   agg(count('*').alias('order_count')).\
-- MAGIC   orderBy(col('order_count').desc()).show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def get_columns(schemas_file, ds_name):
-- MAGIC     schemas_text = spark.read.text(schemas_file, wholetext=True).first()[0]
-- MAGIC     schemas = json.loads(schemas_text)
-- MAGIC     column_details = schemas[ds_name]
-- MAGIC     columns = [col['column_name'] for col in sorted(column_details, key=lambda col: col['column_position'])]
-- MAGIC     return columns

-- COMMAND ----------

-- MAGIC %python
-- MAGIC get_columns('dbfs:/public/retail_db/schemas.json', 'orders')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ds_list = [
-- MAGIC   'departments' , 'categories', 'products', 'customers', 'orders', 'order_items'
-- MAGIC ]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC base_dir = 'dbfs:/public/retail_db'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tgt_base_dir = 'dbfs:/public/retail_db_parquet'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC columns = get_columns(f'{base_dir}/schemas.json', 'orders')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC orders = spark.read.csv(f'{base_dir}/orders', inferSchema=True,).toDF(*columns)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for ds in ds_list:
-- MAGIC     print(f'processing {ds}')
-- MAGIC     columns = get_columns(f'{base_dir}/schemas.json', ds)
-- MAGIC     df = spark.read.csv(f'{base_dir}/{ds}', inferSchema=True,).toDF(*columns)
-- MAGIC     df.write.mode('overwrite').parquet(f'{tgt_base_dir}/{ds}')

-- COMMAND ----------

create table orders(
  order_id INT
  , order_date DATE
  , order_customer_id INT
  , order_status STRING
) using delta
