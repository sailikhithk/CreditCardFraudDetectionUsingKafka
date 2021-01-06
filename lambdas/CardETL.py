import os
from io import BytesIO
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import mean, desc
import pandas as pf
import boto3



spark = SparkSession.builder.appName("ETL").getOrCreate()
sc = spark.sparkContext


df = spark.read.json("denied.json")
df2 = df.orderBy(df["id"])
df = df2.groupBy("id").count()
bucket = 'indepth3'
csv_buffer = BytesIO()
df.toPandas().to_csv(csv_buffer, header=True)
s3_resource = boto3.resource('s3')
s3_resource.Object(bucket, 'df_denied.csv').put(Body=csv_buffer.getvalue())

df = spark.read.json("approved.json")
df2 = df.orderBy(df["buyingPlace"].desc())
dataFrameWay = df2.groupBy("item")
df = dataFrameWay.agg({'amount': 'sum'})
bucket = 'indepth3'
csv_buffer = BytesIO()
df.toPandas().to_csv(csv_buffer, header=True)
s3_resource = boto3.resource('s3')
s3_resource.Object(bucket, 'df.csv').put(Body=csv_buffer.getvalue())

df = spark.read.json("approved.json")
df2 = df.orderBy(df["buyingPlace"].desc())
dataFrameWay = df2.groupBy("id")
df = dataFrameWay.agg({'amount': 'sum'})
bucket = 'indepth3'
csv_buffer = BytesIO()
df.toPandas().to_csv(csv_buffer, header=True)
s3_resource = boto3.resource('s3')
s3_resource.Object(bucket, 'df2.csv').put(Body=csv_buffer.getvalue())
