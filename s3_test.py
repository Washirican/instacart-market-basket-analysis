# Create a SparkSession
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("ReadFromS3") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    aisles_df = spark.read.csv('s3a://washirican-aws-bucket/dataset/aisles.csv', inferSchema=True)
    aisles_df.show()

    spark.stop()

if __name__ == "__main__":
    main()
