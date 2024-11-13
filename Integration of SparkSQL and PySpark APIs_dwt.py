# Databricks notebook source
# MAGIC %sql
# MAGIC DROP DATABASE retail_db CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE retail_db

# COMMAND ----------

# MAGIC %sql
# MAGIC USE retail_db

# COMMAND ----------

# MAGIC %fs ls dbfs:/public/retail_db

# COMMAND ----------

orders = spark.read.csv('dbfs:/public/retail_db/orders',
          schema='order_id INT, order_date DATE, order_customer_id INT, order_status STRING')

# COMMAND ----------

orders.createOrReplaceTempView('orders_v')

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW tables

# COMMAND ----------

display(
  spark.sql("""
            SELECT order_status, COUNT(*) as order_count
            FROM orders_v
            GROUP BY order_status
            ORDER BY order_count DESC
            """)
)

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- SAME SAME BUT DIFFERENT>>>>IN SparkSQL::::
# MAGIC SELECT order_status, COUNT(*) as order_count FROM orders_v GROUP BY order_status ORDER BY order_count DESC

# COMMAND ----------

from pyspark.sql.functions import count,lit

# COMMAND ----------

order_count_by_status = orders.\
  groupBy('order_status').\
    agg(count(lit(1)).alias('order_count'))

# COMMAND ----------

order_count_by_status.write.saveAsTable('order_count_by_status', format = 'delta', mode = 'append')
# append // overwrite based on the requirements

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW tables

# COMMAND ----------

order_count_by_status.write.insertInto('retail_db.order_count_by_status', overwrite = True)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM order_count_by_status

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW tables

# COMMAND ----------

spark.sql("""
          SELECT * FROM retail_db.orders
          """)

# COMMAND ----------

orders_df = spark.read.table('retail_db.orders')

# COMMAND ----------

order_items_df = spark.read.table('retail_db.order_items')
