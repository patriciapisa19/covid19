package covid19

import covid19.utils.CaseClassesUtil.MuertesESP
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.{Expression, StringSplit}
import org.apache.spark.sql.functions.{col, concat_ws, expr, length, lit, split, substring, when,trim,concat,to_date}
import covid19.utils.CreateRDDUtil.spark


object CleanData {

  def hotelesData (hotelesDF: DataFrame): DataFrame = {

    val hotelesRDF = hotelesDF.withColumnRenamed("com_aut_prov","provincia")
    convertProvincia(convertPeriodMes(hotelesRDF))
     // .filter(col("provincia") contains  "Val")

    //.show(20, false)

  }

  def transporteData (transpsDF: DataFrame): DataFrame = {
    convertPeriodMes(transpsDF)

  }

  def tipoHotelData (tipoHotelDF: DataFrame): DataFrame = {
    val tipoHotelRDF = tipoHotelDF.withColumnRenamed("com_aut_prov","provincia")
    convertProvincia(convertPeriodMes(tipoHotelRDF))
      .withColumn("tipo_estancia", when(col("tipo_estancia") contains "Hotelera", "Hoteles")
      .when(col("tipo_estancia") contains "Campings", "Campings")
      .when(col("tipo_estancia") contains "Rural", "Turismo Rural")
      .when(col("tipo_estancia") contains "Apartamentos", "Apartamentos Turísticos"))
      .filter(col("provincia") contains  "Val")
      //.show(20, false)
  }

  def muertesEspData (muertesESPDF: DataFrame): DataFrame = {
    convertSemanaMes(convertProvincia(convertPeriodSemana(muertesESPDF)))
      .withColumn("provincia",
        when(col("provincia") contains "Álava", "Álava")
        .when(col("provincia") contains "Alicante", "Alicante")
        .when(col("provincia") contains "Balear", "Palma de Mallorca")
        .when(col("provincia") contains "Coruña", "La Coruña")
        .when(col("provincia") contains "Castellón", "Castellón")
        .when(col("provincia") contains "Gipuzkoa", "Guipúzcoa")
        .when(col("provincia") === "Lleida", "Lérida")
        .when(col("provincia") contains "Valencia", "Valencia")
        .when(col("provincia") === "Bizkaia", "Vizcaya")
        .when(col("provincia") === "Asturias", "Oviedo")
        .when(col("provincia") === "Cantabria", "Santander")
        .when(col("provincia") contains "Rioja", "Logroño")
        .when(col("provincia") === "Ourense", "Orense")
        .when(col("provincia") === "Girona", "Gerona")
        .otherwise(col("provincia"))
      )
      .withColumn("date", concat(col("year"), lit("-"), (col("month_id"))))
      .withColumn("date",to_date(col("date"),"yyyy-MM"))

    //.filter(col("provincia") contains  "Valencia")


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
      .withColumn("provincia", trim(col("provincia")))
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

  def convertSemanaMes (df: DataFrame) = {
    val semanaMesDF = spark.read.format("csv").option("sep", ";").option("header", "true").load("src/main/resources/relacion_semanas_meses.csv")
    //semanaMesDF.filter(col("year") === "2020").show(100, false)
    df.join(semanaMesDF,Seq("year","week"))
  }

}
