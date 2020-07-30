package covid19.utils

import covid19.utils.CreateRDDUtil.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object ProviCAUtils {

  val provDF: DataFrame = spark.read.format("csv").option("sep", ";").option("header", "true").load("src/main/resources/relacion_ccaa_prov.csv")
  val CADF: DataFrame = spark.read.format("csv").option("sep", ";").option("header", "true").load("src/main/resources/relacion_iso_ccaa.csv")

  val provCADF: DataFrame = provDF.join(CADF, "iso_ccaa")//.filter(col("provincia") contains "Valencia")

}
