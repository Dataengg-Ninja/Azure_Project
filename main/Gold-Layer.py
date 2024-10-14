# Databricks notebook source
from pyspark.sql.functions import*
from pyspark.sql.types import*

# COMMAND ----------

application_id="858876a2-7c06-4702-8805-a57f3d5224a6"
directory_id="d463cc30-76e6-45aa-adc5-f2ae8ce39d11"
storage_account_name="sbitsaproject1"

# COMMAND ----------

secret_key=dbutils.secrets.get(scope="databricks_vault_secret",key="Databricks-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{application_id}",
          "fs.azure.account.oauth2.client.secret": secret_key,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.unmount("/mnt/gold") 

# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://gold-layer@{storage_account_name}.dfs.core.windows.net/",
  mount_point = "/mnt/gold",
  extra_configs = configs)

# COMMAND ----------

spark.read.format("delta").option("inferSchema",True).option("header",True).load("dbfs:/mnt/sliver").createOrReplaceTempView("silver_table")


# COMMAND ----------

sales_data=spark.sql("""select Category,State 
          ,sum(Profit) as Total_profit
           from silver_table group by 1,2
           order by Total_profit desc""")



# COMMAND ----------

sales_data.coalesce(5).write.partitionBy("Category").mode("overwrite").format("delta").save("dbfs:/mnt/gold")

# COMMAND ----------

spark.sql("create database IF NOT EXISTS hive_metastore.sales_db")


# COMMAND ----------

spark.sql("use hive_metastore.sales_db")

# COMMAND ----------

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS category_state_profit
LOCATION 'dbfs:/mnt/gold'
""")



# COMMAND ----------

spark.sql("select * from category_state_profit").show(10)

# COMMAND ----------


