package covid19
import covid19.constants.URLSources
import covid19.utils.CreateSparkSession
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

import scala.collection.immutable



object Covid19 extends App {

  val dataDF = ReadData.dataDF
  dataDF.show(20, truncate = false)

}
