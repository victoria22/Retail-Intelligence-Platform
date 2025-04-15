from pyspark.sql import SparkSession, types, functions as F
from pyspark.sql.functions import col, to_timestamp, expr
from pyspark.sql.window import Window
import os

# Path to the credentials file
credential_location = './keys/credentials.json'
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gs://europe-west1-composer-env-0e6b8b9f-bucket/credentials.json'

# Initialize Spark session with BigQuery and GCS support
spark = SparkSession.builder \
    .appName("Transformation") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .getOrCreate()


# Initialize BigQuery client
project_id = 'retail-intelligence-platform'  # Your PROJECT_ID
dataset_id = 'online_retail'  # Your Dataset name
table_id = 'retail'    # Your Table name
bucket_name = 'retail_intelligence_bucket' # Your Bucket Name

df_spark = spark.read \
    .option("header", "true") \
    .csv('gs://retail_intelligence_bucket/uk_online_retail_data/online_retail.csv') # Path to your GCS data storage

# Count duplicates
df_spark.groupBy(df_spark.columns).count().filter("count > 1").show()

# Count nulls for each column
df_spark.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_spark.columns]).show()

df_spark.schema

schema = types.StructType([
    types.StructField("index", types.IntegerType(), True),
    types.StructField("InvoiceNo", types.StringType(), True),
    types.StructField("StockCode", types.StringType(), True),
    types.StructField("Description", types.StringType(), True),
    types.StructField("Quantity", types.IntegerType(), True),
    types.StructField("InvoiceDate", types.StringType(), True),   
    types.StructField("UnitPrice", types.FloatType(), True),
    types.StructField("CustomerID", types.FloatType(), True),
    types.StructField("Country", types.StringType(), True)
])

# Load the data with new schema
df_spark = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('gs://retail_intelligence_bucket/uk_online_retail_data/online_retail.csv') # Path to GCS Bucket storage

# Explicitly convert InvoiceDate to datetime format
df_spark = df_spark.withColumn("InvoiceDate", expr("try_to_timestamp(InvoiceDate, 'M/d/yyyy H:mm')"))

df_spark.show()  

# Step 1: Filter out negative Quantity and Sales (i.e., Quantity > 0 and Sales > 0)
df_spark_filtered = df_spark.filter((F.col("Quantity") > 0) & (F.col("Quantity") * F.col("UnitPrice") > 0))

# Transform the Data
# Step 1: Identify the most common CustomerID per InvoiceNo (excluding null CustomerIDs)
window_spec_customer = Window.partitionBy("InvoiceNo").orderBy(F.desc("count"))

most_common_customer = (df_spark_filtered
    .filter(F.col("CustomerID").isNotNull())
    .groupBy("InvoiceNo", "CustomerID")
    .count()
    .withColumn("rank", F.row_number().over(window_spec_customer))
    .filter(F.col("rank") == 1)
    .select("InvoiceNo", "CustomerID")
)

# Rename the CustomerID column in most_common_customer to avoid ambiguity
most_common_customer = most_common_customer.withColumnRenamed("CustomerID", "MostCommonCustomerID")

# Step 2: Join back to the original DataFrame to fill missing CustomerID values
df_filled_customer = df_spark_filtered.alias("df").join(
    most_common_customer.alias("mc"),
    on="InvoiceNo",
    how="left"
).withColumn(
    "CustomerID", 
    F.coalesce(F.col("df.CustomerID"), F.col("mc.MostCommonCustomerID"))
)

# Drop 'MostCommonCustomerID' as we no longer need it
df_filled_customer = df_filled_customer.drop("MostCommonCustomerID")

# Option: Handle null CustomerID by setting a default value
default_customer_id = df_spark_filtered.filter(F.col("CustomerID").isNotNull()).groupBy("CustomerID").count().orderBy(F.desc("count")).first()[0]

df_filled_customer = df_filled_customer.withColumn(
    "CustomerID",
    F.when(F.col("CustomerID").isNull(), default_customer_id).otherwise(F.col("CustomerID"))
)

# Step 3: Identify the most common Description per StockCode
window_spec_description = Window.partitionBy("StockCode")

mode_desc = (df_spark_filtered
    .groupBy("StockCode", "Description")
    .count()
    .withColumn("rank", F.row_number().over(Window.partitionBy("StockCode").orderBy(F.desc("count"))))
    .filter(F.col("rank") == 1)
    .select("StockCode", "Description")
)

# Rename Description to avoid ambiguity
most_common_description = mode_desc.withColumnRenamed("Description", "MostCommonDescription")

# Step 4: Join back to the original DataFrame to fill missing Description values
df_filled_final = df_filled_customer.alias("df").join(
    most_common_description.alias("mc"),
    on="StockCode",
    how="left"
).withColumn(
    "Description", 
    F.coalesce(F.col("df.Description"), F.col("mc.MostCommonDescription"))
)

# Drop 'MostCommonDescription' as it's no longer needed
df_filled_final = df_filled_final.drop("MostCommonDescription")

# Handle null Description by setting a default value
default_description = df_spark_filtered.filter(F.col("Description").isNotNull()).groupBy("Description").count().orderBy(F.desc("count")).first()[0]

df_filled_final = df_filled_final.withColumn(
    "Description",
    F.when(F.col("Description").isNull(), default_description).otherwise(F.col("Description"))
)

# Step 5: Extract Date Components
df_filled_final = df_filled_final.withColumn(
    "Year", F.year("InvoiceDate"))

df_filled_final = df_filled_final.withColumn(
    "Month", F.month("InvoiceDate"))

df_filled_final = df_filled_final.withColumn(
    "MonthName", F.date_format("InvoiceDate", "MMMM"))

df_filled_final = df_filled_final.withColumn(
    "DayOfWeek", F.dayofweek("InvoiceDate"))

df_filled_final = df_filled_final.withColumn(
    "NameOfDay", F.date_format("InvoiceDate", "EEEE"))

# Step 6: Compute Sales Volume per Product (including Year and Country)
df_sales = df_filled_final.withColumn("SalesValue", F.col("Quantity") * F.col("UnitPrice"))

# Filter out any negative sales
df_sales = df_sales.filter(F.col("SalesValue") > 0)

sales_volume = df_sales.groupBy("StockCode", "Year", "Country").agg(
    F.sum("SalesValue").alias("TotalSales"),
    F.sum("Quantity").alias("TotalQuantity")
)

# Step 7: Compute Profit Margin per Product (Assume Cost Price = 80% of Unit Price)
df_profit = df_sales.withColumn("CostPrice", F.col("UnitPrice") * 0.8) \
                    .withColumn("ProfitMargin", (F.col("UnitPrice") - F.col("CostPrice")) / F.col("UnitPrice"))

profit_margin = df_profit.groupBy("StockCode", "Year", "Country").agg(
    F.avg("ProfitMargin").alias("AvgProfitMargin"),
    F.sum("SalesValue").alias("TotalSales")
)

# Rename the 'TotalSales' in profit_margin before the join to avoid ambiguity
profit_margin = profit_margin.withColumnRenamed("TotalSales", "Profit_TotalSales")

# Step 8: Partitioned window specs for ranking
window_spec_sales = Window.partitionBy("Year", "Country").orderBy(F.col("TotalSales").desc())
window_spec_profit = Window.partitionBy("Year", "Country").orderBy(F.col("AvgProfitMargin").desc())

# Step 8.1: Classify Sales Volume and store in `sales_volume`
sales_volume = sales_volume.withColumn(
    "SalesCategory_in_sales",  # Renaming it to avoid ambiguity later
    F.when(F.ntile(3).over(window_spec_sales) == 1, "Low")
     .when(F.ntile(3).over(window_spec_sales) == 2, "Medium")
     .otherwise("High")
)

# Step 8.2: Merge Sales and Profit Data
product_performance = sales_volume.alias("sales").join(
    profit_margin.alias("profit"),
    on=["StockCode", "Year", "Country"],
    how="inner"
)

# Step 8.3: Classify Profit Margin within `product_performance`
product_performance = product_performance.withColumn(
    "ProfitCategory",
    F.when(F.ntile(3).over(window_spec_profit) == 1, "Low")
     .when(F.ntile(3).over(window_spec_profit) == 2, "Medium")
     .otherwise("High")
)

# Rename SalesCategory_in_sales to avoid ambiguity during the final select
product_performance = product_performance.withColumnRenamed("SalesCategory_in_sales", "SalesCategory_in_sales_final")

# Ensure the SalesCategory_in_sales column from sales_volume is propagated correctly
product_performance = product_performance.join(
    sales_volume.select("StockCode", "Year", "Country", "SalesCategory_in_sales").alias("sales_volume"),  # Alias to avoid conflict
    on=["StockCode", "Year", "Country"],
    how="left"
)

# Step 9: Merge product performance data with the main dataset
df_final = df_filled_final.alias("main").join(
    product_performance.alias("performance"),
    on=[
        F.col("main.StockCode") == F.col("performance.StockCode"),
        F.col("main.Year") == F.col("performance.Year"),
        F.col("main.Country") == F.col("performance.Country")
    ],
    how="left"
)

# Step 10: Select and cast final columns
df_final = df_final.select(
    F.col("main.index").cast(types.IntegerType()),
    F.col("main.InvoiceNo").cast(types.StringType()),
    F.col("main.StockCode").cast(types.StringType()),
    F.col("main.Description").cast(types.StringType()),
    F.col("main.Quantity").cast(types.IntegerType()),
    F.col("main.InvoiceDate").cast(types.TimestampType()),
    F.col("main.UnitPrice").cast(types.FloatType()),
    F.col("main.CustomerID").cast(types.IntegerType()),  # Changed from FloatType to IntegerType
    F.col("main.Country").cast(types.StringType()),
    F.col("main.Year").cast(types.IntegerType()),
    F.col("main.Month").cast(types.IntegerType()),
    F.col("main.MonthName").cast(types.StringType()),
    F.col("main.DayOfWeek").cast(types.IntegerType()),
    F.col("main.NameOfDay").cast(types.StringType()),
    F.col("performance.Profit_TotalSales").alias("Product_Performance_TotalSales").cast(types.FloatType()),
    F.col("performance.TotalQuantity").cast(types.IntegerType()),
    F.col("performance.AvgProfitMargin").cast(types.FloatType()),
    F.col("performance.SalesCategory_in_sales_final").cast(types.StringType()),  # Correct reference to SalesCategory_in_sales
    F.col("performance.ProfitCategory").cast(types.StringType())
)


# Check the final schema of the dataframe
df_final.printSchema()

# Write to BigQuery
df_final.write.format("bigquery") \
    .option("temporaryGcsBucket", bucket_name) \
    .option("project", project_id) \
    .option("dataset", dataset_id) \
    .option("table", table_id) \
    .option("partitionField", "InvoiceDate")    \
    .option("clusteredFields", "CustomerID")    \
    .mode("overwrite") \
    .save()
