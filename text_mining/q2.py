import re
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
import pyspark.sql.functions as f
from pyspark.sql import Window
from pyspark.sql.types import *


# define schema
schematwo = StructType([StructField("id", LongType(), True), StructField("title",StringType(), True), StructField('revision', StructType([StructField('text', StringType(), True) ]))])

# get table
df = spark.read.format("com.databricks.spark.xml").option("ignoreSurroundingSpaces","true").option("rowTag", "page").load("/Users/lizheqing/Downloads/Wiki_data_dump_32GB.xml", schema=schematwo)

df.select("revision.text").show()

# function to get number of urls
def regex_filter(x):
	return len(re.findall(r'(https?://[^\s]+)', x))

# define udf
regex_udf = udf(lambda z: regex_filter(z), IntegerType())

# use udf to add a col of url number
url_table = df.select('title','id','revision.text',regex_udf('revision.text').alias('urls'))

# select query to get results
url_table.where(url_table.urls > 5).show()
