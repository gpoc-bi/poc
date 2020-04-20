from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import *
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import asc
from pyspark.sql.functions import col
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame
from pyspark.sql.functions import sha2, concat_ws
from pyspark.sql import readwriter
from pyspark.sql import utils 

 
sc = SparkContext("local", "count app")
SQLContext = SQLContext(sc)

spark = SparkSession.builder. \
    master('local'). \
    appName('foo'). \
    getOrCreate()
spark.sparkContext.setLogLevel('WARN')

 
#Reading source and lrf
df_source = spark.read.csv("gs://bi-gcp-poc-bucket/sample.csv", inferSchema= True , header= True)
df_source.show()


# Saving the data to BigQuery
df_source.write.format('bigquery') \
  .option('table', 'dataset_1.table_1') \
  .option("temporaryGcsBucket","bi-gcp-poc-bucket") \
  .mode('append') \
.save()
