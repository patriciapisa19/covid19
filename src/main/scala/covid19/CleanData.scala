package covid19

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.{Expression, StringSplit}
import org.apache.spark.sql.functions.{col, expr, lit, split, when, substring,length,concat_ws}

object CleanData {

  def hotelesData (hotelesDF: DataFrame): DataFrame = {

    val provinciasDF = hotelesDF
      //.filter(col("com_aut_prov") contains  "La")
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

    val periodoDF: DataFrame = convertPeriod(provinciasDF)

    //periodoDF.show(20, false)
    periodoDF
  }

  def transporteData (transpsDF: DataFrame): DataFrame = {
    convertPeriod(transpsDF)
  }

  def convertPeriod(df: DataFrame) : DataFrame = {
    df.withColumn("periodo", split(col("periodo"),"M"))
      .withColumn("year",col("periodo")(0))
      .withColumn("month",col("periodo")(1))
      .drop("periodo")

  }

}
