package covid19.utils

import covid19.utils.CreateRDDUtil.spark
import org.apache.spark.sql.DataFrame

object ContinentUtils {

  val continentDF: DataFrame = spark.read.format("csv").option("sep", ";").option("header", "true").load("src/main/resources/relacion_continentes.csv")

}
