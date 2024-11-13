-- Databricks notebook source
-- MAGIC %fs ls dbfs:/public/retail_db/schemas.json

-- COMMAND ----------

select * from text.`dbfs:/public/retail_db/schemas.json`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC schemas_text = spark.read.text("dbfs:/public/retail_db/schemas.json", wholetext=True).first().value

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import json

-- COMMAND ----------

-- MAGIC %python
-- MAGIC json.loads(schemas_text).keys()

-- COMMAND ----------

select * from csv.`dbfs:/public/retail_db/orders/part-00000`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC column_details = json.loads(schemas_text)['orders']
-- MAGIC columns = [col['column_name'] for col in sorted( column_details, key=lambda col: col['column_position'])]
-- MAGIC columns
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC orders = spark.read.csv('dbfs:/public/retail_db/orders', inferSchema=True).toDF(*columns)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import count

-- COMMAND ----------

-- MAGIC %python
-- MAGIC orders.groupby('order_status').agg(count('*')).show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def get_columns(schemas_file,ds_name):
-- MAGIC   schemas_text = spark.read.text(schemas_file, wholetext=True).first().value
-- MAGIC   schemas = json.loads(schemas_text)
-- MAGIC   column_details = schemas[ds_name]
-- MAGIC   columns = [col['column_name'] for col in sorted(column_details, key=lambda col: col['column_position'])]
-- MAGIC   return columns
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC get_columns('dbfs:/public/retail_db/schemas.json','orders')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ds_list = ['orders','order_items','customers','departments','products']

-- COMMAND ----------

-- MAGIC %python
-- MAGIC base_dir = 'dbfs:/public/retail_db'
-- MAGIC tgt_base_dir = 'dbfs:/public/retail_db_parquet'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for ds in ds_list:
-- MAGIC   print(f'Loading {ds}...')
-- MAGIC   columns = get_columns(f'{base_dir}/schemas.json', ds)
-- MAGIC   print(columns)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for ds in ds_list:
-- MAGIC   print(f'Processing {ds}...')
-- MAGIC   columns = get_columns(f'{base_dir}/schemas.json', ds)
-- MAGIC   df = spark.read.csv(f'{base_dir}/{ds}', inferSchema = True).toDF(*columns)
-- MAGIC   df.write.mode('overwrite').parquet(f'{tgt_base_dir}/{ds}')

-- COMMAND ----------


