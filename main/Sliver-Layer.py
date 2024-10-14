# Databricks notebook source
from pyspark.sql.functions import*
from pyspark.sql.types import*

# COMMAND ----------

application_id="858876a2-7c06-4702-8805-a57f3d5224a6"
directory_id="d463cc30-76e6-45aa-adc5-f2ae8ce39d11"
container_name="bronze-layer"
storage_account_name="sbitsaproject1"

# COMMAND ----------

secret_key=dbutils.secrets.get(scope="databricks_vault_secret",key="Databricks-secret")

# COMMAND ----------

dbutils.fs.unmount("/mnt/sliver") 
dbutils.fs.unmount("/mnt/bronze") 

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{application_id}",
          "fs.azure.account.oauth2.client.secret": secret_key,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs)




# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://silver-layer@{storage_account_name}.dfs.core.windows.net/",
  mount_point = "/mnt/sliver",
  extra_configs = configs)

# COMMAND ----------

df_Order_Details=spark.read.format("csv").option("inferSchema",True).option("header",True).load("dbfs:/mnt/bronze/Order Details/")
df_ListofOrders=spark.read.format("csv").option("inferSchema",True).option("header",True).load("dbfs:/mnt/bronze/List of Orders/")
df_Sales_target=spark.read.format("csv").option("inferSchema",True).option("header",True).load("dbfs:/mnt/bronze/Sales target/")



# COMMAND ----------

df_Order_Details=df_Order_Details.filter(col("Profit")>0)

# COMMAND ----------

df1=df_Order_Details.join(df_ListofOrders,on=df_Order_Details['Order ID']==df_ListofOrders['Order ID'],how="inner")

# COMMAND ----------

df2=df1.join(df_Sales_target,on=df1.Category==df_Sales_target.Category,how="inner")
df2.show()

# COMMAND ----------

df3=df2.drop(df_ListofOrders['Order ID'],df_Sales_target.Category).dropDuplicates()

# COMMAND ----------

df4=df3.withColumn("Month",split(col("Month_of_Order_Date"),"-")[0])


# COMMAND ----------

df4.show()

# COMMAND ----------

df4_renamed = df4 \
    .withColumnRenamed("Order ID", "Order_ID") \
    .withColumnRenamed("Sub-Category", "Sub_Category") \
    .withColumnRenamed("Order Date", "Order_Date") 
   

# COMMAND ----------

df4_renamed.coalesce(5) \
    .write \
    .partitionBy("Category") \
    .mode("overwrite") \
    .format("delta") \
    .save("dbfs:/mnt/sliver")

