

dependencies to download:
Python 2.7.10
Spark version 2.4.3 Using Scala version 2.11.12, Java HotSpot(TM) 64-Bit Server VM, 1.8.0_131


how to execute: codes are directly run on shell
how to start shell:
scala: 
$SPARK_HOME/bin/spark-shell --packages com.databricks:spark-xml_2.12:0.5.0
sql: 
spark-sql --packages com.databricks:spark-xml_2.10:0.4.1
python: 
pyspark --master local --packages com.databricks:spark-xml_2.10:0.4.1


