#!/usr/bin/env python
# coding: utf-8

# Import Packages

# In[ ]:


from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window
import os

# Following packages are used for testing purposes
# import pytest
# import ipytest
# ipytest.autoconfig()


# Call Spark Session

# In[ ]:
current_directory = os.getcwd()
# path_to_hadoop = os.path.join(current_directory, '..', 'utils', 'hadoop')
os.environ["HADOOP_HOME"] = current_directory+"/utils/hadoop"
# print(os.path.abspath(path_to_hadoop))
# os.environ["HADOOP_HOME"] = os.path.abspath(path_to_hadoop)
# os.environ['PYSPARK_PYTHON'] = '/path/to/python3.9'
# os.environ['PYSPARK_DRIVER_PYTHON'] = '/path/to/python3.9'


spark = SparkSession.builder\
        .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5")\
        .config("spark.sql.legacy.timeParserPolicy","LEGACY")\
        .appName("order_analysis_tdd") \
        .getOrCreate()


# Read Data function using spark. raw data format, schema and dbfs path are parameters

# In[ ]:


def read_data(spark, format,schema, path):
    if format == "json":
        df = spark.read.format(format).schema(schema).option("multiline", "true").load(path) #schema is specified for reading json data.
    else:
        df = spark.read.format(format).option("header",True).option("inferSchema",True).load(path) #Used default inferSchema option to read data in csv and xlsx format
    return df


# Spark Write Function on Hive table. Write mode, data format and table name are parameters

# In[ ]:


def write_function(df, mode, format, table_name):
    try:
        df.write \
            .mode(mode) \
            .format(format) \
            .option("header",True)\
            .save("D:/Programs/TDD project/order_analysis_tdd/output_data/"+table_name)
        print(f"DataFrame is written to {table_name}.")
    except Exception as e:
        print(f"An error occurred while writing the DataFrame: {e}")


# Order enrichment function which is a joined dataset of Orders, Products and customers. 

# In[ ]:


# Profit column is rounded to 2 decimal points and Order Year column is created.
def order_enrichment(df_orders, df_customers, df_products):
    df_orders_sel = df_orders.withColumn("Order Year", year(to_timestamp(col("Order Date"), "dd/mm/yyyy")))\
                                                .withColumn("Profit", round(col("Profit"),2))
    df_cust_sel = df_customers.select(col("Customer ID"), col("Customer Name"), col("Country"))
    df_prod_sel = df_products.select(col("Product ID"), col("Category"), col("Sub-Category")).distinct()
    
    df_orders_enrich = df_orders_sel.join(df_cust_sel, ["Customer ID"], "inner").join(df_prod_sel, ["Product ID"], "left")
    
    return df_orders_enrich
    
# Bug with Product dataset
# Duplicate Product IDs - Same Product ID have multiple records with different name and location
# few missing Product ID's in product table but are in Orders


# Function to aggregate the order_enriched table to calculate `Total Profit` based on different group by fields.
# Final result is ordered by `Total Profit` in descending order

# In[ ]:


def profit(df_order_enriched, field):
    df = df_order_enriched.groupBy(field).agg(sum("Profit").alias("Total Profit"))
    df_round = df.withColumn("Total Profit", round(col("Total Profit"),2))
    return df_round.orderBy(col("Total Profit").desc())


# Schema to read json data. I have used default inderschema option in spark to read data from other data formats like xlsx and csv.

# In[ ]:


schema = StructType([StructField("Row ID", IntegerType(), True),
                      StructField("Order ID", StringType(), True),
                      StructField("Order Date", StringType(), True),
                      StructField("Ship Date", StringType(), True),
                      StructField("Ship Mode", StringType(), True),
                      StructField("Customer ID", StringType(), True),
                      StructField("Product ID", StringType(), True),
                      StructField("Quantity", IntegerType(), True),
                      StructField("Price", FloatType(), True),
                      StructField("Discount", FloatType(), True),
                      StructField("Profit", FloatType(), True)])


# Read raw data using read_data function. Spark Session, data format, schema, path to file are parameters.

# In[ ]:


df_customers = read_data(spark, format="com.crealytics.spark.excel", schema=None, path="D:/Programs/TDD project/order_analysis_tdd/data/Customer.xlsx")
df_products = read_data(spark, format="csv", schema=None, path="D:/Programs/TDD project/order_analysis_tdd/data/Product.csv")
df_orders = read_data(spark, format="json",schema=schema,path="D:/Programs/TDD project/order_analysis_tdd/data/Order.json")


# Data transformations

# In[ ]:


# Call function `order_enrichment`. raw dataframes of orders, customers, products are specified as parameters.
df_order_enriched = order_enrichment(df_orders, df_customers, df_products)


# In[ ]:


# Call function `profit` to calulate profit by specific fields and stored as different dataframes. 
# df_order_enriched and field column (Year, Category, Sub-category, Customer Name) for grouping is specified.
df_profit_by_year = profit(df_order_enriched=df_order_enriched, field="Order Year")
df_profit_by_product_category =  profit(df_order_enriched=df_order_enriched, field="Category")
df_profit_by_product_sub_category = profit(df_order_enriched=df_order_enriched, field="Sub-Category")
df_profit_by_customer = profit(df_order_enriched=df_order_enriched, field="Customer Name")


# Write data to tables

# In[ ]:


# Call function `write_function` to write the required dataframes to tables using Hive.
# I have used parquet as dataformat to write on for better compressibility. 
# Respective Table name is also specified along with write mode as overwrite
# Raw tables, order enriched table and profit by category tables are created.
write_function(df=df_orders,  mode="overwrite", format="csv", table_name="Orders")
write_function(df=df_products,  mode="overwrite", format="csv", table_name="Products")
write_function(df=df_customers,  mode="overwrite", format="csv", table_name="Customers")

write_function(df=df_order_enriched,  mode="overwrite", format="csv", table_name="Orders_enriched")

write_function(df=df_profit_by_year,  mode="overwrite", format="csv", table_name="Profit_by_Year")
write_function(df=df_profit_by_product_category,  mode="overwrite", format="csv", table_name="Profit_by_Product_Category")
write_function(df=df_profit_by_product_sub_category,  mode="overwrite", format="csv", table_name="Profit_by_Product_Sub_Category")
write_function(df=df_profit_by_customer,  mode="overwrite", format="csv", table_name="Profit_by_Customer")
