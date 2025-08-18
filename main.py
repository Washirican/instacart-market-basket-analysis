from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, count, desc


# Initialize Spark session
spark = SparkSession.builder \
    .appName("Instacart Market Basket Analysis") \
    .getOrCreate()

# Load data files into DataFrames
aisles_df = spark.read.csv("dataset/aisles.csv", header=True, inferSchema=True)
departments_df = spark.read.csv(
    "dataset/departments.csv", header=True, inferSchema=True)
order_products_df = spark.read.csv(
    "dataset/order_products__prior.csv", header=True, inferSchema=True)
orders_df = spark.read.csv("dataset/orders.csv", header=True, inferSchema=True)
products_df = spark.read.csv(
    "dataset/products.csv", header=True, inferSchema=True)

products_details_df = products_df.join(aisles_df, "aisle_id") \
                                 .join(departments_df, "department_id") \
                                 .select("product_id",
                                         "product_name",
                                         "aisle",
                                         "department")

products_details_df.show()