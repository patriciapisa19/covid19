package covid19

import covid19.utils.ContinentUtils._
import covid19.utils.CreateRDDUtil.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType}

object CleanDataOWID {

  def casosMundiales (casosMundDF : DataFrame) : DataFrame = {
    val casosDateDF = convertDate(casosMundDF)
    casosDateDF.printSchema()
    val casosDropColDF = casosDateDF.withColumn("continent", when(col("location") === "World", "World")
      .when(col("location") === "International", "International")
      .otherwise(col("continent")))
      .drop("iso_code").drop("new_cases_smoothed")
      .drop("new_deaths_smoothed").drop("total_cases_per_million")
      .drop("new_cases_per_million").drop("new_cases_smoothed_per_million")
      .drop("total_deaths_per_million").drop("new_deaths_per_million")
      .drop("new_deaths_smoothed_per_million").drop("total_tests_per_thousand")
      .drop("new_tests_per_thousand").drop("new_tests_smoothed")
      .drop("new_tests_smoothed_per_thousand").drop("tests_per_case")
      .drop("positive_rate").drop("tests_units")
      .drop("stringency_index").drop("median_age")
      .drop("aged_65_older").drop("aged_70_older")
      .drop("gdp_per_capita").drop("extreme_poverty")
      .drop("cardiovasc_death_rate").drop("diabetes_prevalence")
      .drop("female_smokers").drop("male_smokers")
      .drop("handwashing_facilities").drop("hospital_beds_per_thousand")
      .drop("life_expectancy").drop("human_development_index")
      .drop("weekly_icu_admissions").drop("weekly_icu_admissions_per_million")
      .drop("hosp_patients").drop("hosp_patients_per_million")
      .drop("icu_patients").drop("icu_patients_per_million")
      .drop("weekly_hosp_admissions").drop("weekly_hosp_admissions_per_million")


    val casosFloatDF = casosDropColDF
      .withColumn("new_cases",col("new_cases").cast(FloatType))
      .withColumn("new_deaths",col("new_deaths").cast(FloatType))
      .withColumn("new_tests",col("new_tests").cast(FloatType))
      .withColumn("population",col("population").cast(FloatType))
      .withColumn("population_density",col("population_density").cast(FloatType))
      .withColumn("total_cases",col("total_cases").cast(FloatType))
      .withColumn("total_deaths",col("total_deaths").cast(FloatType))
      .withColumn("total_tests",col("total_tests").cast(FloatType))

    casosFloatDF.printSchema()
    casosFloatDF.show(300, false)

    //    val decimalNum1DF = dropPoint(casosDropColDF,"new_cases")
//    val decimalNum2DF = dropPoint(decimalNum1DF,"new_deaths")
//    val decimalNum3DF = dropPoint(decimalNum2DF,"new_tests")
//    val decimalNum4DF = dropPoint(decimalNum3DF,"population")
//    val decimalNum5DF = dropPoint(decimalNum4DF,"population_density")
//    val decimalNum6DF = dropPoint(decimalNum5DF,"total_cases")
//    val decimalNum7DF = dropPoint(decimalNum6DF,"total_deaths")
//    val decimalNum8DF = dropPoint(decimalNum7DF,"total_tests")

    val casosDF = casosFloatDF.join(continentDF,"continent")
    casosDF.printSchema()
    casosDF.show(300, false)
    casosDF.printSchema()
    casosDF
  }

  def movilidadMund (movilidadDF : DataFrame) : DataFrame = {
    val movDateDF = convertDate(movilidadDF)
    movDateDF.printSchema()
    val movDF = movDateDF.drop("sub_region_1").drop("sub_region_2").drop("metro_area")
      .drop("iso_3166_2_code").drop("census_fips_code")
        .withColumnRenamed("retail_and_recreation_percent_change_from_baseline", "comercio_recreación")
        .withColumnRenamed("grocery_and_pharmacy_percent_change_from_baseline","supermercados_farmacia")
        .withColumnRenamed("parks_percent_change_from_baseline","parques")
        .withColumnRenamed("transit_stations_percent_change_from_baseline", "estaciones")
        .withColumnRenamed("workplaces_percent_change_from_baseline", "trabajo")
        .withColumnRenamed("residential_percent_change_from_baseline", "residencia")
        .withColumnRenamed("country_region", "location")

      .withColumn("comercio_recreación", col("comercio_recreación").cast(IntegerType))
        .withColumn("supermercados_farmacia", col("supermercados_farmacia").cast(IntegerType))
        .withColumn("parques", col("parques").cast(IntegerType))
        .withColumn("estaciones", col("estaciones").cast(IntegerType))
        .withColumn("trabajo", col("trabajo").cast(IntegerType))
        .withColumn("residencia", col("residencia").cast(IntegerType))


    //movDF.show(20, false)
    movDF.groupBy("location").count().show(150,false)
    movDF
  }

  def traficoAereoInternacional(trafAereoDF: DataFrame): DataFrame ={
    val traficoDF = trafAereoDF.withColumn("tra_meas",
      when(col("tra_meas") === "PAS_BRD_DEP", "Passengers on board (departures)")
        .when(col("tra_meas") === "PAS_BRD", "Passengers on board")
        . when(col("tra_meas") === "PAS_CRD_ARR", "Passengers carried (arrival)")
        . when(col("tra_meas") === "PAS_CRD", "Passengers carried")
        . when(col("tra_meas") === "CAF_PAS", "Commercial passenger air flights")
        . when(col("tra_meas") === "CAF_PAS_DEP", "Commercial passenger air flights (departures)")
        . when(col("tra_meas") === "CAF_PAS_ARR", "Commercial passenger air flights (arrivals)")
        . when(col("tra_meas") === "PAS_BRD_ARR", "Passengers on board (arrivals)")
        . when(col("tra_meas") === "PAS_CRD_DEP", "Passengers carried (departures)")
    )

    traficoDF.show(200,false)
    traficoDF.groupBy("geo_time").count().show(100,false)

    traficoDF
  }


  def convertDate(df: DataFrame) : DataFrame = {
    df.withColumn("date1", split(col("date"),"-"))
      .withColumn("year",col("date1")(0).cast(IntegerType))
      .withColumn("month",col("date1")(1).cast(IntegerType))
      .withColumn("day",col("date1")(2).cast(IntegerType))
      .drop("date1")


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



}
