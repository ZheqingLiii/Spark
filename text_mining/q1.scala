import libraries in scala:
import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._
import spark.implicits._
import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType}

// read xml file and extract tags
val df=spark.read.option("rowTag","page").option("rowTag","revision").xml("/Users/lizheqing/Downloads/Wiki_data_dump_32GB.xml")
// count number of minor tags
df.filter($"minor"!=="null").count()
