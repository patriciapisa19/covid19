package covid19.utils
import CreateRDDUtil.spark
import org.apache.spark.sql.DataFrame

object ProviCAUtils {

  val provDF: DataFrame = spark.read.format("csv").option("header", "true").load("src/main/resources/relacion_ccaa_prov.csv")
  val CADF: DataFrame = spark.read.format("csv").option("header", "true").load("src/main/resources/relacion_iso_ccaa.csv")

  val provCADF = provDF.join(CADF, "iso_ccaa")



}
