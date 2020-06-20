package covid19

import org.apache.spark.sql.SparkSession

object CreateSparkSession {
  // Create the Spark Session and the spark context
  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  // Get the Spark context from the Spark session for creating the streaming context
  //  val sc = spark.sparkContext
  //  // Create the streaming context
  //  val ssc = new StreamingContext(sc, Seconds(10))
}
