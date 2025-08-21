# Create a SparkSession
from pyspark.sql import SparkSession


def main():
    # TODO (2025-08-21): Fix to use JAR files
    # TODO (2025-08-21): Fix to use credentials from environment variables
    spark = SparkSession.builder \
        .appName("ReadFromS3") \
        .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

    aisles_df = spark.read.csv('s3a://washirican-aws-bucket/dataset/aisles.csv', inferSchema=True)
    aisles_df.show()

    spark.stop()

if __name__ == "__main__":
    main()
    # Run with this:
    # spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 s3_test.py
