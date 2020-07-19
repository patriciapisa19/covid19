package covid19

import covid19.utils.CaseClassesUtil.MuertesESP
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.{Expression, StringSplit}
import org.apache.spark.sql.functions.{col, concat_ws, expr, length, lit, split, substring, when}

object CleanData {

  def hotelesData (hotelesDF: DataFrame): Unit = {

    val hotelesRDF = hotelesDF.withColumnRenamed("com_aut_prov","provincia")
    convertProvincia(convertPeriodMes(hotelesRDF))
      .filter(col("provincia") contains  "Bal")
      .show(20, false)

  }

  def transporteData (transpsDF: DataFrame): DataFrame = {
    convertPeriodMes(transpsDF)

  }

  def tipoHotelData (tipoHotelDF: DataFrame): Unit = {
    val tipoHotelRDF = tipoHotelDF.withColumnRenamed("com_aut_prov","provincia")
    convertProvincia(convertPeriodMes(tipoHotelRDF))
      .withColumn("tipo_estancia", when(col("tipo_estancia") contains "Hotelera", "Hoteles")
      .when(col("tipo_estancia") contains "Campings", "Campings")
      .when(col("tipo_estancia") contains "Rural", "Turismo Rural")
      .when(col("tipo_estancia") contains "Apartamentos", "Apartamentos Turísticos"))
      .filter(col("provincia") contains  "Bal")
      .show(20, false)
  }

  def muertesEspData (muertesESPDF: DataFrame): Unit = {
    convertProvincia(convertPeriodSemana(muertesESPDF))
      .filter(col("provincia") contains  "Bal")
      .show(20, false)

  }


  def convertProvincia(df: DataFrame) = {
    df.withColumn("provincia",
      when(col("provincia").startsWith("T"), "00 Total Nacional")
        otherwise(col("provincia")))
      .withColumn("provincia", expr("substring(provincia,4,length(provincia))"))
      .withColumn("provinciaAux",
        when(col("provincia") contains(","), split(col("provincia")," ")(0))
          otherwise(""))
      .withColumn("provinciaAux", expr("substring(provinciaAux, 1, (length(provinciaAux) - 1 ))"))
      .withColumn("provincia",
        when(col("provincia") contains ",", (split(col("provincia"),", ")(1)))
          otherwise(col("provincia")))
      .withColumn("provincia", concat_ws(" ", col("provincia"),col("provinciaAux")))
      .drop("provinciaAux")
  }

  def convertPeriodMes(df: DataFrame) : DataFrame = {
    df.withColumn("periodo", split(col("periodo"),"M"))
      .withColumn("year",col("periodo")(0))
      .withColumn("month",col("periodo")(1))
      .drop("periodo")

  }

  def convertPeriodSemana(df: DataFrame) : DataFrame = {
    df.withColumn("periodo", split(col("periodo"),"SM"))
      .withColumn("year",col("periodo")(0))
      .withColumn("week",col("periodo")(1))
      .drop("periodo")
  }

}
