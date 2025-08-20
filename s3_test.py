from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf


AWS_ACCESS_KEY="****"
AWS_SECRET_KEY="***"


jar_path ="aws-java-sdk-bundle-1.12.788.jar,hadoop-aws-3.4.1.jar"


spark = SparkSession.builder \
    .appName("S3SparkIntegration") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.executor.extraClassPath", jar_path) \
    .config("spark.jars",jar_path) \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true") \
    .config("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.2.0,"
            "com.amazonaws:aws-java-sdk-bundle:1.11.375") \
    .getOrCreate()  # Use getOrCreate() to prevent multiple Spark contexts


df = spark.read.csv("s3://washirican-aws-bucket/dataset/t/aisles.csv", header=True, inferSchema=True)
df.show()

