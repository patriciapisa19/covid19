package covid19

import covid19.utils.CaseClassesUtil.MuertesESP
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.{Expression, StringSplit}
import org.apache.spark.sql.functions.{col, concat, concat_ws, count, expr, length, lit, regexp_extract, regexp_replace, split, substring, to_date, trim, when}
import covid19.utils.CreateRDDUtil.spark
import org.apache.spark.sql.types.{FloatType, IntegerType}
import covid19.utils.ProviCAUtils._


object CleanData {

//  def hotelesData (hotelesDF: DataFrame): DataFrame = {
//
//    val hotelesRDF = hotelesDF.withColumnRenamed("com_aut_prov","provincia")
//    convertProvincia(convertPeriodMes(hotelesRDF))
//     // .filter(col("provincia") contains  "Val")
//
//    //.show(20, false)
//
//  }

  def transporteData (transpsDF: DataFrame): DataFrame = {
    dropPoint(convertPeriodMes(transpsDF),"total")
  }

  def tipoHotelData (tipoHotelDF: DataFrame): DataFrame = {

    //convertPeriodMes(tipoHotelDF).groupBy("ccaa").count().show(50,false)
    //nameCCAA(convertPeriodMes(tipoHotelDF),"ccaa").groupBy("ccaa").count().show(50,false)
    //dropPoint(nameCCAA(convertPeriodMes(tipoHotelDF),"ccaa"),"total").groupBy("ccaa").count().show(50,false)

    val dataDF = convertCCAA(tipoHotelDF).withColumn("tipo_estancia", when(col("tipo_estancia") contains "Hotelera", "Hoteles")
      .when(col("tipo_estancia") contains "Campings", "Campings")
      .when(col("tipo_estancia") contains "Rural", "Turismo Rural")
      .when(col("tipo_estancia") contains "Apartamentos", "Apartamentos Turísticos"))

    //dataDF.groupBy("ccaa").count().show(50,false)
    val hotelDF =  dropPoint(nameCCAA(convertPeriodMes(dataDF),"ccaa"),"total")

    hotelDF.groupBy("ccaa").count().show(50,false)
    val joinDF = hotelDF.join(CADF, "ccaa")


    //joinDF.show(20,false)
    joinDF
  }

  def muertesEspData (muertesESPDF: DataFrame): DataFrame = {
    dropPoint(convertSemanaMes(convertProvincia(convertPeriodSemana(muertesESPDF))),"total")
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

  def convertCCAA(df: DataFrame) = {
    df.withColumn("ccaa",
      when(col("ccaa").startsWith("T"), "00 Total Nacional")
        otherwise(col("ccaa")))
      .withColumn("ccaa", expr("substring(ccaa,4,length(ccaa))"))
      .withColumn("ccaaAux",
        when(col("ccaa") contains(","), split(col("ccaa")," ")(0))
          otherwise(""))
      .withColumn("ccaaAux", expr("substring(ccaaAux, 1, (length(ccaaAux) - 1 ))"))
      .withColumn("ccaa",
        when(col("ccaa") contains ",", (split(col("ccaa"),", ")(1)))
          otherwise(col("ccaa")))
      .withColumn("ccaa", concat_ws(" ", col("ccaa"),col("ccaaAux")))
      .withColumn("ccaa", trim(col("ccaa")))
      .drop("ccaaAux")

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

  def dropPoint (df: DataFrame, nameColumn: String): DataFrame = {
//    df.withColumn("newColumn", regexp_replace(df(nameColumn), "(\\.)", "").cast(IntegerType))
//      .drop(nameColumn).withColumnRenamed("newColumn",nameColumn)
//      .withColumn("newColumn", regexp_replace(df(nameColumn), "(\\,)", ".").cast(FloatType))
//      .drop(nameColumn).withColumnRenamed("newColumn",nameColumn)

    df.withColumn(nameColumn, when(col(nameColumn) rlike  "(\\.)", regexp_replace(df(nameColumn), "(\\.)", ""))
      .when(col(nameColumn) rlike  "(\\,)", regexp_replace(df(nameColumn), "(\\,)", "."))
      .otherwise(col(nameColumn)).cast(FloatType))

  }

  def nameProvincias (df: DataFrame, columnLocation: String): DataFrame = {
    df.withColumn(columnLocation,
      when(col(columnLocation) contains "Álava", "Álava")
        .when(col(columnLocation) contains "Alicante", "Alicante")
        .when(col(columnLocation) contains "Balear", "Palma de Mallorca")
        .when(col(columnLocation) contains "Coruña", "La Coruña")
        .when(col(columnLocation) contains "Castellón", "Castellón")
        .when(col(columnLocation) contains "Gipuzkoa", "Guipúzcoa")
        .when(col(columnLocation) === "Lleida", "Lérida")
        .when(col(columnLocation) contains "Valencia", "Valencia")
        .when(col(columnLocation) === "Bizkaia", "Vizcaya")
        .when(col(columnLocation) === "Asturias", "Oviedo")
        .when(col(columnLocation) === "Cantabria", "Santander")
        .when(col(columnLocation) contains "Rioja", "Logroño")
        .when(col(columnLocation) === "Ourense", "Orense")
        .when(col(columnLocation) === "Girona", "Gerona")
        .otherwise(col(columnLocation)))
  }

  def nameCCAA (df: DataFrame, columnLocation: String) : DataFrame = {
    df.withColumn(columnLocation,
        when(col(columnLocation) contains "Mancha", "Castilla-La Mancha")
        .when(col(columnLocation) contains "Navarra", "Comunidad Foral Navarra")
        .when(col(columnLocation) contains "Balear", "Islas Baleares")
        .when(col(columnLocation) contains "Valenciana", "Comunidad Valenciana")
        .otherwise(col(columnLocation)))
  }

}
