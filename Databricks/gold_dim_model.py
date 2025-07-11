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
# MAGIC select DISTINCT(Model_id) as Model_id, Model_Category from parquet.`abfss://silver@salesdatalake27.dfs.core.windows.net/carsales`

# COMMAND ----------

df_src = spark.sql('''select DISTINCT(Model_id) as Model_id, Model_Category from parquet.`abfss://silver@salesdatalake27.dfs.core.windows.net/carsales`''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim_model sink initial and incremental

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    df_sink = spark.sql('''select dim_model_id, Model_Id , Model_Category FROM cars_catalog.gold.dim_model''')
else:
    df_sink = spark.sql('''select 1 as dim_model_id, Model_Id , Model_Category FROM parquet.`abfss://silver@salesdatalake27.dfs.core.windows.net/carsales` WHERE 1=0''')

# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bringing already exisiting records

# COMMAND ----------

df_filter = df_src.join(df_sink,df_src['Model_id'] == df_sink['Model_id'],'left').select(df_src['Model_ID'], df_src['Model_Category'], df_sink['dim_model_id'])

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **df_filter_old**

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_model_id').isNotNull())


# COMMAND ----------

# MAGIC %md
# MAGIC **df_filter_new**

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_model_id').isNull()).select(col('Model_ID'),col('Model_Category'))

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
    max_value_df = spark.sql('''Select max(dim_model_id) from cars_catalog.gold.dim_model''')
    max_value = max_value_df.collect()[0][0] + 1

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_model_id',max_value + monotonically_increasing_id())

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

if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    delta_table = DeltaTable.forPath(spark, 'abfss://gold@salesdatalake27.dfs.core.windows.net/dim_model')
    delta_table.alias('trg').merge(df_final.alias('src'), 'trg.dim_model_id = src.dim_model_id')\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_final.write.format('delta')\
        .mode('overwrite')\
        .option('path','abfss://gold@salesdatalake27.dfs.core.windows.net/dim_model')\
        .saveAsTable('cars_catalog.gold.dim_model')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_model;

# COMMAND ----------

