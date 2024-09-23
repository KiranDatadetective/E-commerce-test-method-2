from app.Order_pyspark_test import spark, order_enrichment, profit
from pyspark.sql.functions import col, round, sum


def test_order_enrichment(spark=spark):
    # dummy dataframes are created using spark which included necessary corner cases like multiple orders, customers, products, product table duplication, profit with long decimal point
    df_orders_raw_list = [("1", "order_id1", "21/8/2016", "25/8/2016", "Standard Class", "cust_id1", "prod_id1", "7",
                           "573.174", "0.3", "63.686"),
                          ("2", "order_id2", "21/8/2017", "25/8/2017", "First Class", "cust_id2", "prod_id2", "3",
                           "573.174", "0.3", "50.112")]

    df_customers_raw_list = [("cust_id1", "cust_name1", "email1", "00000000", "address1", "Consumer", "country1",
                              "city1", "state1", "80027.0", "West"),
                             ("cust_id2", "cust_name2", "email1", "00000000", "address2", "Consumer", "country2",
                              "city2", "state2", "80027.0", "West")]

    df_products_raw_list = [("prod_id1", "Furniture", "Chairs", "prod_name1", "New York", "81.882"),
                            ("prod_id2", "Technology", "Accessories", "prod_name2", "New York", "81.882"),
                            ("prod_id3", "Technology", "Accessories", "prod_name3", "New York", "81.882")]

    df_orders_raw = spark.createDataFrame(df_orders_raw_list,
                                          ["Row ID", "Order ID", "Order Date", "Ship Date", "Ship Mode", "Customer ID",
                                           "Product ID", "Quantity", "Price", "Discount", "Profit"])
    df_customers_raw = spark.createDataFrame(df_customers_raw_list,
                                             ["Customer ID", "Customer Name", "email", "phone", "address", "Segment",
                                              "Country", "City", "State", "Postal Code", "Region"])
    df_products_raw = spark.createDataFrame(df_products_raw_list,
                                            ["Product ID", "Category", "Sub-Category", "Product Name", "State",
                                             "Price per product"])

    # dummy dataframe are created with expected results based on the input data.
    df_enrichment_final_raw_list = [("prod_id1", "cust_id1", "1", "order_id1", "21/8/2016", "25/8/2016",
                                     "Standard Class", "7", "573.174", "0.3", "63.69", "2016", "cust_name1", "country1",
                                     "Furniture", "Chairs"),
                                    ("prod_id2", "cust_id2", "2", "order_id2", "21/8/2017", "25/8/2017", "First Class",
                                     "3", "573.174", "0.3", "50.11", "2017", "cust_name2", "country2", "Technology",
                                     "Accessories")]

    df_enrichment_final_expected = spark.createDataFrame(df_enrichment_final_raw_list,
                                                         ["Product ID", "Customer ID", "Row ID", "Order ID",
                                                          "Order Date", "Ship Date", "Ship Mode", "Quantity", "Price",
                                                          "Discount", "Profit", "Order Year", "Customer Name",
                                                          "Country", "Category", "Sub-Category"])

    # Calling the order_enrichment function and execute based on our dummy dataframe and fetch the result
    df_enrichment_final_result = order_enrichment(df_orders_raw, df_customers_raw, df_products_raw)

    # Checking if result table count is equal to expected table count
    assert df_orders_raw.count() == df_enrichment_final_result.count()

    # Checking if result table have customer name based on the ID and matched with raw data.
    # Also checked if the Profit is rounded to 2 decimal points
    assert df_enrichment_final_result.select(col("Customer Name"), col("Profit")).where(
        col("Customer Name") == "cust_name1").collect()[0][1] == \
           df_orders_raw.select(col("Customer ID"), round(col("Profit"), 2)).where(
               col("Customer ID") == "cust_id1").collect()[0][1]

    # Checking if the result table is entirely equal to the dummy expected table
    assert df_enrichment_final_result.orderBy(col("Row ID").asc()).exceptAll(
        df_enrichment_final_expected.orderBy(col("Row ID").asc())).count() == 0

    assert df_enrichment_final_expected.orderBy(col("Row ID").asc()).exceptAll(
        df_enrichment_final_result.orderBy(col("Row ID").asc())).count() == 0


# In[ ]:


# Testing function to test Profit function
# get_ipython().run_line_magic('%ipytest', '')

def test_profit(spark=spark):
    # dummy data for order enriched table is created
    df_order_enriched_raw_list = [("prod_id1", "cust_id1", "1", "order_id1", "21/8/2016", "25/8/2016", "Standard Class",
                                   "7", "573.174", "0.3", "63.69", "2016", "cust_name1", "country1", "Furniture",
                                   "Chairs"),
                                  ("prod_id2", "cust_id2", "2", "order_id2", "21/8/2017", "25/8/2017", "First Class",
                                   "3", "573.174", "0.3", "50.11", "2017", "cust_name2", "country2", "Technology",
                                   "Accessories"),
                                  ("prod_id3", "cust_id2", "3", "order_id3", "21/8/2017", "25/8/2017", "First Class",
                                   "3", "573.174", "0.3", "50.11", "2017", "cust_name2", "country2", "Technology",
                                   "Accessories")]

    df_order_enriched_raw = spark.createDataFrame(df_order_enriched_raw_list,
                                                  ["Product ID", "Customer ID", "Row ID", "Order ID", "Order Date",
                                                   "Ship Date", "Ship Mode", "Quantity", "Price", "Discount", "Profit",
                                                   "Order Year", "Customer Name", "Country", "Category",
                                                   "Sub-Category"])

    # dummy data was created with expected results based on the input dummy data.
    df_profit_per_year_final_expected = spark.createDataFrame([("2016", "63.69"), ("2017", "100.22")],
                                                              ["Order Year", "Total Profit"])
    df_profit_per_category_final_expected = spark.createDataFrame([("Furniture", "63.69"), ("Technology", "100.22")],
                                                                  ["Category", "Total Profit"])
    df_profit_per_sub_category_final_expected = spark.createDataFrame([("Chairs", "63.69"), ("Accessories", "100.22")],
                                                                      ["Sub-Category", "Total Profit"])
    df_profit_per_customer_final_expected = spark.createDataFrame([("cust_name1", "63.69"), ("cust_name2", "100.22")],
                                                                  ["Customer Name", "Total Profit"])

    # Calling the profit function and execute based on our dummy raw  order enriched and fetch the result
    df_profit_per_year_final_result = profit(df_order_enriched=df_order_enriched_raw, field="Order Year")
    df_profit_per_category_final_result = profit(df_order_enriched=df_order_enriched_raw, field="Category")
    df_profit_per_sub_category_final_result = profit(df_order_enriched=df_order_enriched_raw, field="Sub-Category")
    df_profit_per_customer_final_result = profit(df_order_enriched=df_order_enriched_raw, field="Customer Name")

    # Checking if the Total Profit in the aggregated function is equal to total profit in raw order data
    assert df_profit_per_year_final_result.select(sum("Total Profit")).collect()[0][0] == \
           df_order_enriched_raw.select(sum("Profit")).collect()[0][0]

    # Checking if Result table is equal to Expected table. This is done for all 4 aggregations.
    assert df_profit_per_year_final_result.exceptAll(
        df_profit_per_year_final_expected.orderBy(col("Total Profit").desc())).count() == 0
    assert df_profit_per_category_final_result.exceptAll(
        df_profit_per_category_final_expected.orderBy(col("Total Profit").desc())).count() == 0
    assert df_profit_per_sub_category_final_result.exceptAll(
        df_profit_per_sub_category_final_expected.orderBy(col("Total Profit").desc())).count() == 0
    assert df_profit_per_customer_final_result.exceptAll(
        df_profit_per_customer_final_expected.orderBy(col("Total Profit").desc())).count() == 0
