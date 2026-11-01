# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### SILVER LAYER SCRIPT

# COMMAND ----------

# MAGIC %md
# MAGIC #### DATA ACCESS USING APP

# COMMAND ----------
#Databricks <- Data Lake
#doc: https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage
# 1. Microsoft Entra ID -> App registrations (application id and directory (tenant) ID are needed)
# 2. Microsoft Entra ID -> Certificates and secrets (secret is needed)
# 3. Storage -> Access Control -> Add a role "storage BLOB Data Contributer" <- add app (created in 1) as a member

# 4. add <storage-account> = storage, <application-id> = application id from 1, <directory-id> = directory (tenant) ID from 1, service_credential = secret from 2

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")
# COMMAND ----------

# MAGIC %md
# MAGIC #### DATA LOADING

# COMMAND ----------

# MAGIC %md
# MAGIC Read Calendar Data
# MAGIC

# COMMAND ----------

df_cal = spark.read.format('csv')\
            .option("header", True)\
            .option("inferSchema", True)\
            .load("abfss://bronze@awstoragedatalakevp.dfs.core.windows.net/AdventureWorks_Calendar")

# COMMAND ----------

df_cal.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Read All Data

# COMMAND ----------

base_path = "abfss://bronze@awstoragedatalakevp.dfs.core.windows.net/"

dfs = {}

folders = dbutils.fs.ls(base_path)

for folder in folders:
    if folder.isDir():
        folder_name = folder.name.rstrip("/").lstrip("AdventureWorks_").lower()
        if "sales_" not in folder_name:

            df = spark.read.format('csv')\
                .option("header", True)\
                .option("inferSchema", True)\
                .load(folder.path)

            dfs[f"df_{folder_name}"] = df

#sales mix
df_sales = spark.read.format('csv')\
            .option("header", True)\
            .option("inferSchema", True)\
            .load("abfss://bronze@awstoragedatalakevp.dfs.core.windows.net/AdventureWorks_Sales*")

dfs["df_sales"] = df_sales


# COMMAND ----------

dfs

# COMMAND ----------

# MAGIC %md
# MAGIC #### TRANSFORMATIONS

# COMMAND ----------

#Create "Month" column in df_calendar
dfs["df_calendar"] = dfs["df_calendar"].withColumn('Month', date_format(col("Date"), "MMMM"))\
    .withColumn('Year', year(col("Date")))
    
dfs["df_calendar"].display()

# COMMAND ----------

#add df_calendar to silver
#silver_path = "abfss://silver@awstoragedatalakevp.dfs.core.windows.net/"

#dfs["df_calendar"].write.format('parquet')\
    # .mode('append')\
    # .option("path", silver_path + "AdventureWorks_Calendar")\
    # .save()

# COMMAND ----------

#add "FullName" column in df_customers
dfs["df_customers"] = dfs["df_customers"].withColumn('FullName', concat_ws(' ', col('Prefix'), col('FirstName'), col('LastName')))
dfs["df_customers"].display()

# COMMAND ----------

#modify df_products: "ProductSKU" and "ProductName" columns
dfs["df_products"] = dfs["df_products"].withColumn('ProductSKU', split(col('ProductSKU'), '-')[0])\
    .withColumn('ProductName', split(col('ProductName'), ' ')[0])


dfs["df_products"].display()

# COMMAND ----------

#change time format in df_sales, StockDate column
#OrderNumber column, replace 'S' with 'T
#add "Multiply" column

dfs['df_sales'] = dfs['df_sales'].withColumn('StockDate', to_timestamp('StockDate'))\
    .withColumn('OrderNumber', regexp_replace(col('OrderNumber'), 'S', 'T'))\
        .withColumn('Multiply', col('OrderLineItem')*col('OrderQuantity'))


#Sales analysis, count of orders per day

dfs['df_sales'].groupBy('OrderDate').agg(count('OrderNumber').alias('TotalOrders')).display()

dfs['df_sales'].display()


# COMMAND ----------

dfs['df_product_categories'].display()

# COMMAND ----------

dfs['df_territories'].display()

# COMMAND ----------

#add all data to silver
silver_path = "abfss://silver@awstoragedatalakevp.dfs.core.windows.net/"

for df in dfs:
    dfs[df].write.format('parquet')\
        .mode("append")\
        .option("path", silver_path + f"AdventureWorks_{df.strip('df_')}")\
        .save()