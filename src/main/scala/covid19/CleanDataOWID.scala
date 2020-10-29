package covid19

import covid19.utils.ContinentUtils._
import covid19.utils.CreateRDDUtil.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType}

object CleanDataOWID {

  def casosMundiales (casosMundDF : DataFrame) : DataFrame = {
    val casosDateDF = convertDate(casosMundDF)
    casosDateDF.withColumn("continent", when(col("location") === "World", "World")
      .when(col("location") === "International", "International")
      .otherwise(col("continent")))
    //val casosDecimalDF = dropPoint(casosDateDF, "")

    val casosDF = casosDateDF.join(continentDF,"continent")

    casosDateDF
  }





  def transporteData (transpsDF: DataFrame): DataFrame = {
    dropPoint(convertPeriodMes(transpsDF),"total")
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

  def convertDate(df: DataFrame) : DataFrame = {
    df.withColumn("date", split(col("date"),"-"))
      .withColumn("year",col("date")(0))
      .withColumn("month",col("date")(1))
      .withColumn("day",col("date")(2).cast(IntegerType))
      .drop("date")

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
