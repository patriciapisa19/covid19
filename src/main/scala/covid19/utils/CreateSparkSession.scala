package covid19.utils

import org.apache.spark.sql.SparkSession

object CreateSparkSession {
  // Create the Spark Session
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
    .getOrCreate()
}
