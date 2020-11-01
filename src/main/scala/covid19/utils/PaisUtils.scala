package covid19.utils

import covid19.utils.CreateRDDUtil.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object PaisUtils {

      val paisDF: DataFrame = spark.read.format("csv").option("sep", ";").option("header", "true").load("src/main/resources/relacion_iso_pais.csv")

      paisDF.show(20,false)

}
