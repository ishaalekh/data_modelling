# Databricks notebook source
# MAGIC %md
# MAGIC # Reading the data

# COMMAND ----------

df = spark.read.format('parquet').load('abfss://bronze@salesdatalake27.dfs.core.windows.net/rawdata')

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformation

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import * 

# COMMAND ----------

df = df.withColumn('Model_Category', split(col('Model_ID'),'-')[0])
df.display()

# COMMAND ----------

df.withColumn('Units_Sold',col('Units_Sold').cast(StringType())).printSchema()

# COMMAND ----------

df = df.withColumn('Revenue_Per_Unit',col('Revenue')/col('Units_Sold'))

# COMMAND ----------

df.display()

# COMMAND ----------

df.groupBy('Year','BranchName').agg(sum('Units_Sold').alias('Total_Units_Sold')).sort('Year','Total_Units_Sold',ascending=[1,0]).display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format('parquet').mode('overwrite').option('path','abfss://silver@salesdatalake27.dfs.core.windows.net/carsales').save()

# COMMAND ----------

# MAGIC %md
# MAGIC # Querying data from the silver layer
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@salesdatalake27.dfs.core.windows.net/carsales`

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from parquet.`abfss://silver@salesdatalake27.dfs.core.windows.net/carsales`;