# Databricks notebook source
# MAGIC %md
# MAGIC # Create Flag Parameter

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Dimension Model 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch Relative Columns

# COMMAND ----------

from pyspark.sql.functions import *


# COMMAND ----------

# MAGIC %sql
# MAGIC select DISTINCT(Date_id) as Date_id from parquet.`abfss://silver@salesdatalake27.dfs.core.windows.net/carsales`

# COMMAND ----------

df_src = spark.sql('''select DISTINCT(Date_id) as Date_id from parquet.`abfss://silver@salesdatalake27.dfs.core.windows.net/carsales`''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim_model sink initial and incremental

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_date'):
    df_sink = spark.sql('''select dim_date_id, Date_id FROM cars_catalog.gold.dim_date''')
else:
    df_sink = spark.sql('''select 1 as dim_date_id, Date_id FROM parquet.`abfss://silver@salesdatalake27.dfs.core.windows.net/carsales` WHERE 1=0''')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bringing already exisiting records

# COMMAND ----------

df_filter = df_src.join(df_sink,df_src['Date_id'] == df_sink['Date_id'],'left').select(df_src['Date_id'], df_sink['dim_date_id'])

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **df_filter_old**

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_date_id').isNotNull())


# COMMAND ----------

# MAGIC %md
# MAGIC **df_filter_new**

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_date_id').isNull()).select(col('Date_id'))

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Create Surrogate key

# COMMAND ----------

# MAGIC %md
# MAGIC **fetching max surrogate key**

# COMMAND ----------

if incremental_flag == '0':
    max_value = 1
else: 
    max_value_df = spark.sql('''Select max(dim_date_id) from cars_catalog.gold.dim_date''')
    max_value = int(max_value_df.collect()[0][0]) + 1
    

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_date_id',max_value + monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Final df

# COMMAND ----------

df_final = df_filter_old.union(df_filter_new)


# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update or insert

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_date'):
    delta_table = DeltaTable.forPath(spark, 'abfss://gold@salesdatalake27.dfs.core.windows.net/dim_date')
    delta_table.alias('trg').merge(df_final.alias('src'), 'trg.dim_date_id = src.dim_date_id')\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_final.write.format('delta')\
        .mode('overwrite')\
        .option('path','abfss://gold@salesdatalake27.dfs.core.windows.net/dim_date')\
        .saveAsTable('cars_catalog.gold.dim_date')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_date;

# COMMAND ----------

