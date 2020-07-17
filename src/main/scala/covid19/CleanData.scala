package covid19

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.{Expression, StringSplit}
import org.apache.spark.sql.functions.{col, expr, lit, split, when, substring,length,concat_ws}

object CleanData {

  def hotelesData (hotelesDF: DataFrame): DataFrame = {

    val provinciasDF = hotelesDF.filter(col("com_aut_prov") contains  "La")
      .withColumn("provincia",
        when(col("com_aut_prov").startsWith("T"), "00 Total Nacional")
          otherwise(col("com_aut_prov")))
      .withColumn("provincia", expr("substring(provincia,4,length(com_aut_prov))"))
      .withColumn("provinciaAux",
        when(col("provincia") contains(","), split(col("provincia")," ")(0))
          otherwise(""))
      .withColumn("provinciaAux", expr("substring(provinciaAux, 1, (length(provinciaAux) - 1 ))"))
      .withColumn("provincia",
        when(col("provincia") contains ",", (split(col("provincia"),", ")(1)))
          otherwise(col("provincia")))
      .withColumn("provincia", concat_ws(" ", col("provincia"),col("provinciaAux")))
      .drop("provinciaAux")
      .drop("com_aut_prov")



    val periodoDF = provinciasDF.withColumn("periodo2", split(col("periodo"),"M"))
      .selectExpr("provincia","viajeros_penoct","residencia","total","periodo2[1]", "periodo2[0]")
      .withColumnRenamed("periodo2[0]", "year")
      .withColumnRenamed("periodo2[1]", "month")

    periodoDF
  }

}
