# Databricks notebook source
# MAGIC %md
# MAGIC # Create Fact Table

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading Silver Data**

# COMMAND ----------

df_silver = spark.sql("select * from parquet.`abfss://silver@salesdatalake27.dfs.core.windows.net/carsales`")

# COMMAND ----------

df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the dim tables**

# COMMAND ----------

df_model = spark.sql('select * from cars_catalog.gold.dim_model')
df_dealer = spark.sql('select * from cars_catalog.gold.dim_dealer')
df_branch = spark.sql('select * from cars_catalog.gold.dim_branch')
df_date = spark.sql('select * from cars_catalog.gold.dim_date')

# COMMAND ----------

# MAGIC %md
# MAGIC **Bringing keys to the fact table**

# COMMAND ----------

df_fact = df_silver.join(df_model, df_model['Model_id']==df_silver['Model_id'],'left')\
        .join(df_dealer, df_dealer['Dealer_id']==df_silver['Dealer_id'],'left')\
        .join(df_branch, df_branch['Branch_id']==df_silver['Branch_id'],'left')\
        .join(df_date, df_date['Date_id']==df_silver['Date_id'],'left').select(df_model['dim_model_id'],df_dealer['dim_dealer_id'],df_branch['dim_branch_id'],df_date['dim_date_id'],df_silver['Revenue'],df_silver['Revenue_Per_Unit'],df_silver['Units_Sold'])

# COMMAND ----------

df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating Fact SCD**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.factSales'):
    delta_table = DeltaTable.forPath(spark,'abfss://gold@salesdatalake27.dfs.core.windows.net/factSales')
    delta_table.alias('trg').merge(df_fact.alias('src'),'trg.dim_model_id=src.dim_model_id and trg.dim_dealer_id=src.dim_dealer_id and trg.dim_branch_id=src.dim_branch_id and trg.dim_date_id=src.dim_date_id')\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_fact.write.format('delta').mode('overwrite').option('path','abfss://gold@salesdatalake27.dfs.core.windows.net/factSales').saveAsTable('cars_catalog.gold.factSales')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.factsales;

# COMMAND ----------

