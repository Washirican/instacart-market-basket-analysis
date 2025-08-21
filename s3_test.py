# Create a SparkSession
from pyspark.sql import SparkSession
# INCOMPLETE (2025-08-21): This code runs from command line but not directly from VS Code.

def main():
    # TODO (2025-08-21): Fix to use JAR files
    # TODO (2025-08-21): Fix to use credentials from environment variables
    spark = SparkSession.builder \
        .appName("ReadFromS3") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

    aisles_df = spark.read.csv(
        's3a://washirican-aws-bucket/dataset/aisles.csv', header=True, inferSchema=True)
    aisles_df.show()

    departments_df = spark.read.csv(
        "s3a://washirican-aws-bucket/dataset/departments.csv", header=True, inferSchema=True)
    departments_df.show()

    # order_products_df = spark.read.csv("s3a://washirican-aws-bucket/dataset/order_products__prior.csv", header=True, inferSchema=True)
    # orders_df = spark.read.csv("s3a://washirican-aws-bucket/dataset/orders.csv", header=True, inferSchema=True)

    products_df = spark.read.csv(
        "s3a://washirican-aws-bucket/dataset/products.csv", header=True, inferSchema=True)
    products_df.show()

    products_details_df = products_df.join(aisles_df, "aisle_id") \
        .join(departments_df, "department_id") \
        .select("product_id",
                "product_name",
                "aisle",
                "department")

    products_details_df.show()

    # TODO (2025-08-21): Save result table to S3 bucket.

    spark.stop()


if __name__ == "__main__":
    main()
    # Run with this:
    # spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 s3_test.py
