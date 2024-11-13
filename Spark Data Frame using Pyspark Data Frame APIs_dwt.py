# Databricks notebook source
# MAGIC %md
# MAGIC ### - Spark Data Frame using Pyspark Data Frame APIs
# MAGIC ### - Basic Transformations using Pyspark Data Frame APIs
# MAGIC ### - Ranking using pySpatk Data Frame APIs
# MAGIC

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# Overview of the usage of Dataframes ::
sales = [[125000,10],[9200,12], [16300,9]]
sales_df = pd.DataFrame(sales, columns = ['sale_amount', 'comission_pct'])
sales_df.apply(lambda row: (row.sale_amount * row.comission_pct)/100, axis = 1)

# COMMAND ----------

df = spark.read.csv('dbfs:/public/retail_db/orders', schema='order_id INT, order_date STRING, order_customer_id INT, order_status STRING')

# COMMAND ----------

df.printSchema 
        # - shows schema details
# df.show() - shows all data
        # df.columns - shows columns

# COMMAND ----------

display(df.select('order_date', 'order_status').
  distinct().
  orderBy('order_date', 'order_status').
  count())

# COMMAND ----------

display(df.drop('order_customer_id'))

# COMMAND ----------

from pyspark.sql.functions import col, cast, date_format
display(df.select('order_id', 'order_date', cast('int', date_format('order_date', 'yyyyMM')).alias('order_month')))

# Import the functions we need from the pyspark.sql.functions module :: col, cast, date_format.
# These functions in the pyspark.sql.functions module are available and they can be used to process data using select clause

# COMMAND ----------

# df_with_month = df.withColumn('order_month', cast('int', date_format('order_date', 'yyyyMM')))
          #this cast is part of pyspark.sql.functions module
#  This doesnt cast the column to int, so we need to use .cast function)
df_with_month = df.withColumn('order_month', date_format('order_date', 'yyyyMM').cast('int'))
display(df_with_month)
df_with_month.printSchema()
# CAUTION::::If you use a same column name it replaces the existing column.

# COMMAND ----------

display(df.drop('order_customer_id').withColumn('order_month', cast('int', date_format('order_date', 'yyyyMM'))))

# COMMAND ----------

# %python
# df.write.format('delta').save('dbfs:/public/retail_db_delta/orders_with_month')

# COMMAND ----------

%fs ls dbfs:/public/retail_db_delta/orders

# COMMAND ----------

orders_df = spark.read.csv('dbfs:/public/retail_db/orders', schema = 'order_id INT, order_date STRING, order_customer_id INT, order_status STRING')

# COMMAND ----------

display(orders_df)

# COMMAND ----------

from pyspark.sql.functions import date_format, cast

# COMMAND ----------

display(orders_df.withColumn('order_month', date_format('order_date', 'yyyyMM')))

# COMMAND ----------

orders_df.filter("order_status = 'COMPLETE'").count()

# COMMAND ----------

# Problem statement :: Get data for closed and complete orders in month 2014 Jan
display(orders_df.filter("date_format(order_date, 'yyyyMM') == 201401 AND order_status IN ('COMPLETE', 'CLOSED')"))

# COMMAND ----------

display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Aggregations by Key
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import count,col
display(orders_df.\
  groupBy("order_status").\
  agg(count('order_id').alias('order_count')).\
  orderBy(col('order_count').desc()) )

# COMMAND ----------

order_items_df = spark.read.csv('dbfs:/public/retail_db/order_items',
        schema = 'order_item_id INT, order_item_order_id INT, order_item_product_id INT, order_item_quantity INT, order_item_subtotal FLOAT, order_item_product_price FLOAT',)

# COMMAND ----------

from pyspark.sql.functions import sum, round
display(order_items_df.\
  groupBy("order_item_order_id").\
    agg(round(sum('order_item_subtotal'), 2).alias('order_revenue')).\
      orderBy('order_item_order_id'))


# COMMAND ----------

from pyspark.sql.functions import count, date_format
display(orders_df.\
  groupBy(date_format('order_date', 'yyyyMM').cast('int').alias('order_month')).
  agg(count('order_id').alias('order_count')).\
    orderBy('order_month'))

# COMMAND ----------

from pyspark.sql.functions import col
display(order_items_df.\
  orderBy('order_item_order_id', col('order_item_subtotal').desc()))

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/online_retail

# COMMAND ----------

online_df= spark.read.csv('dbfs:/databricks-datasets/online_retail/data-001', header = True, inferSchema = True)

# COMMAND ----------

display(online_df)

# COMMAND ----------

online_df.filter('customerID is null').count()

# COMMAND ----------

# Dealing with nulls
display(online_df.filter('stockCode = 71053').\
  orderBy(col('customerID').asc_nulls_last()))

# COMMAND ----------

display(orders_df)

# COMMAND ----------

display(order_items_df)

# COMMAND ----------

# SELECT * FROM ORDERS AS o 
#   JOIN ORDER_ITEMS AS oi ON o.order_id = oi.order_item_order_id

order_details_df = orders_df.join(order_items_df,orders_df.order_id == order_items_df.order_item_order_id)

# COMMAND ----------

display(order_details_df)

# COMMAND ----------

# SELECT o.*, oi.order_item_subtotal FROM ORDERS AS o 
#   JOIN ORDER_ITEMS AS oi ON o.order_id = oi.order_item_order_id

display(order_details_df.select(orders_df['*'], order_items_df['order_item_subtotal']))
# Or if you need just some columns then mention them explicitly....

# COMMAND ----------

# SELECT o.*, oi.round(sum(order_item_subtotal),2) FROM ORDERS AS o 
#   JOIN ORDER_ITEMS AS oi ON o.order_id = oi.order_item_order_id
      # GROUP BY o.order_date
      # WHERE order_status IN ("COMPLETE", "CLOSED") AND date_format(order_date, "yyyyMM") = 201401 
      # GROUP BY o.order_date
      # ORDER BY revenue DESC
from pyspark.sql.functions import col
display(
orders_df.filter('order_status IN ("COMPLETE", "CLOSED") AND date_format(order_date, "yyyyMM") = 201401').\
  join(order_items_df, orders_df['order_id'] == order_items_df['order_item_order_id']).\
    groupBy('order_date').\
      agg(round(sum('order_item_subtotal'),2).alias('revenue')).\
        orderBy(col('revenue').desc()))

# COMMAND ----------

# Outer join
display(orders_df.join(order_items_df, orders_df['order_id'] == order_items_df['order_item_order_id'], 'left'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##            ----Ranking----

# COMMAND ----------

daily_revenue = orders_df.filter('order_status IN ("COMPLETE", "CLOSED")').\
  join(order_items_df, orders_df['order_id'] == order_items_df['order_item_order_id']).\
  groupBy('order_date').\
  agg(round(sum('order_item_subtotal'),2).alias('revenue'))

# COMMAND ----------

daily_product_revenue_df = orders_df.filter('order_status IN ("COMPLETE", "CLOSED")').join(order_items_df, orders_df['order_id'] == order_items_df['order_item_order_id']).groupBy('order_date', 'order_item_product_id').agg(round(sum('order_item_subtotal'),2).alias('revenue')) 

# COMMAND ----------

from pyspark.sql.functions import date_format, col, dense_rank
from pyspark.sql.window import Window
display (
    daily_product_revenue_df.\
      filter(date_format('order_date', 'yyyyMM')== '201401').\
      withColumn('drnk', dense_rank().over(Window.orderBy(col('revenue').desc()))).
      orderBy(('order_date'), col('revenue').desc())
)

# COMMAND ----------

display (
    daily_product_revenue_df.\
      filter("order_date == '2014-01-01 00:00:00.0'").\
      withColumn('drnk', dense_rank().over(Window.orderBy(col('revenue').desc()))).
      filter("drnk <= 5").\
      orderBy('order_date', col('revenue').desc())
)

# COMMAND ----------

spec = Window.partitionBy('order_date').orderBy(col('revenue').desc())
# first partitition by and then order by :::ORDER
display (
    daily_product_revenue_df.\
      filter("order_date == '2014-01-01 00:00:00.0'").\
      withColumn('drnk', dense_rank().over(spec)).
      filter("drnk <= 5").\
      orderBy('order_date', col('revenue').desc())
)

# COMMAND ----------

display (
    daily_product_revenue_df.\
      filter("order_date BETWEEN '2014-01-01' AND '2014-03-31'").\
      withColumn('drnk', dense_rank().over(spec)).
      filter("drnk <= 3").\
      orderBy('order_date', col('revenue').desc())
)
# Mostly for problem statements which require any top 3 / 5 data dense rank is used for that.

# COMMAND ----------


