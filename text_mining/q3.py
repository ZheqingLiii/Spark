import re
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
import pyspark.sql.functions as f
from pyspark.sql import Window
from pyspark.sql.types import *


# define schema
schemathree = StructType([StructField("id", LongType(), True),StructField("timestamp", StringType(), True),StructField("contributor", StructType([StructField("username", StringType(), True), StructField("id", LongType(), True) ]))])

# read xml file
df_t = spark.read.format("com.databricks.spark.xml").option("ignoreSurroundingSpaces","true").option("rowTag", "page").option("rowTag", "revision").load("/Users/lizheqing/Downloads/Wiki_data_dump_32GB.xml", schema=schemathree)

# create a table group by contributor id
count_c = df_t.groupBy("contributor.id").count()

# create another table with renamed id columns
tb = df_t.select(col('id').alias('revision_id'),'timestamp',col('contributor.id').alias('c_id'),'contributor.username').orderBy("timestamp",ascending=False)

# outer join two tables based on contributor id
joined_df = tb.join(count_c, tb.c_id==count_c.id,'outer')

# get results. counter > 1 means this contributor appeared more than once in the revision
joined_df.filter(col("count")>1).show() 
