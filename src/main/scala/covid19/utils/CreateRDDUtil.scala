package covid19.utils

import covid19.utils.CaseClassesUtil._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object CreateRDDUtil {

  val spark: SparkSession = CreateSparkSession.spark

  //INE

  def createDFHotelESP(records: List[List[String]] ): DataFrame = {
    val recordsRDD: RDD[CaseClassesUtil.HotelesESP] = spark.sparkContext.parallelize(records) //se convierte en RDD
      .map(register => CaseClassesUtil.HotelesESP(
        Try(register.head).getOrElse("null"),
        Try(register(1)).getOrElse("null"),
        Try(register(2)).getOrElse("null"),
        Try(register(3)).getOrElse("null"),
        Try(register(4).toInt).getOrElse(0)
      ))
     spark.sqlContext.createDataFrame(recordsRDD)

  }

  def createDFTipoHotelESP(records: List[List[String]] ): DataFrame = {
    val recordsRDD: RDD[CaseClassesUtil.TiposHotelESP] = spark.sparkContext.parallelize(records) //se convierte en RDD
      .map(register => CaseClassesUtil.TiposHotelESP(
        Try(register.head).getOrElse("null"),
        Try(register(1)).getOrElse("null"),
        Try(register(2)).getOrElse("null"),
        Try(register(3)).getOrElse("null"),
        Try(register(4)).getOrElse("null"),
        Try(register(5).toInt).getOrElse(0)
      ))
    spark.sqlContext.createDataFrame(recordsRDD)

  }

  def createDFTranspESP(records: List[List[String]]): DataFrame = {
    val recordsRDD: RDD[CaseClassesUtil.TransporteESP] = spark.sparkContext.parallelize(records) //se convierte en RDD
      .map(register => CaseClassesUtil.TransporteESP(
        Try(register.head).getOrElse("null"),
        Try(register(1)).getOrElse("null"),
        Try(register(2)).getOrElse("null"),
        Try(register(3).toInt).getOrElse(0)
      ))
    spark.sqlContext.createDataFrame(recordsRDD)
  }

    def createDFMuertesESP(records: List[List[String]]): DataFrame = {
      val recordsRDD: RDD[CaseClassesUtil.MuertesESP] = spark.sparkContext.parallelize(records) //se convierte en RDD
        .map(register => CaseClassesUtil.MuertesESP(
          Try(register.head).getOrElse("null"),
          Try(register(1)).getOrElse("null"),
          Try(register(2)).getOrElse("null"),
          Try(register(3)).getOrElse("null"),
          Try(register(4)).getOrElse("null"),
          Try(register(5).toInt).getOrElse(0)
        ))
      spark.sqlContext.createDataFrame(recordsRDD)
  }

  def createDFCasosESP(records: List[List[String]]): DataFrame = {
    val recordsRDD: RDD[CasosESP] = spark.sparkContext.parallelize(records) //se convierte en RDD
      .map(register => CasosESP(
        Try(register.head).getOrElse("null"),
        Try(register(1)).getOrElse("null"),
        Try(register(2).toInt).getOrElse(0),
        Try(register(3).toInt).getOrElse(0),
        Try(register(4).toInt).getOrElse(0),
        Try(register(5).toInt).getOrElse(0),
        Try(register(6).toInt).getOrElse(0)))
    spark.sqlContext.createDataFrame(recordsRDD)
  }

  //Our World In Data

  def createDFCasosMund(records: List[List[String]]): DataFrame = {
    val recordsRDD: RDD[CasosMund] = spark.sparkContext.parallelize(records) //se convierte en RDD
      .map(register => CasosMund(
        Try(register.head).getOrElse("null"), Try(register(1)).getOrElse("null"),
        Try(register(2)).getOrElse("null"), Try(register(3)).getOrElse("null"),
        Try(register(4)).getOrElse("null"), Try(register(5)).getOrElse("null"),
        Try(register(6)).getOrElse("null"), Try(register(7)).getOrElse("null"),
        Try(register(8)).getOrElse("null"), Try(register(9)).getOrElse("null"),
        Try(register(10)).getOrElse("null"), Try(register(11)).getOrElse("null"),
        Try(register(12)).getOrElse("null"), Try(register(13)).getOrElse("null"),
        Try(register(14)).getOrElse("null"), Try(register(15)).getOrElse("null"),
        Try(register(16)).getOrElse("null"), Try(register(17)).getOrElse("null"),
        Try(register(18)).getOrElse("null"), Try(register(19)).getOrElse("null"),
        Try(register(20)).getOrElse("null"), Try(register(21)).getOrElse("null"),
        Try(register(22)).getOrElse("null"), Try(register(23)).getOrElse("null"),
        Try(register(24)).getOrElse("null"), Try(register(25)).getOrElse("null"),
        Try(register(26)).getOrElse("null"), Try(register(27)).getOrElse("null"),
        Try(register(28)).getOrElse("null"), Try(register(29)).getOrElse("null"),
        Try(register(30)).getOrElse("null"), Try(register(31)).getOrElse("null"),
        Try(register(32)).getOrElse("null"), Try(register(33)).getOrElse("null"),
        Try(register(34)).getOrElse("null"), Try(register(35)).getOrElse("null"),
        Try(register(36)).getOrElse("null"), Try(register(37)).getOrElse("null"),
        Try(register(38)).getOrElse("null"), Try(register(39)).getOrElse("null"),
        Try(register(40)).getOrElse("null"), Try(register(41)).getOrElse("null"),
        Try(register(42)).getOrElse("null"), Try(register(43)).getOrElse("null"),
        Try(register(44)).getOrElse("null"), Try(register(45)).getOrElse("null"),
        Try(register(46)).getOrElse("null"), Try(register(47)).getOrElse("null"),
        Try(register(48)).getOrElse("null")))
    spark.sqlContext.createDataFrame(recordsRDD)
  }

  def createDFMovilidadMund(records: List[List[String]]): DataFrame = {
    val recordsRDD: RDD[MovilidadMund] = spark.sparkContext.parallelize(records) //se convierte en RDD
      .map(register => MovilidadMund(
        Try(register.head).getOrElse("null"),
        Try(register(1)).getOrElse("null"),
        Try(register(2)).getOrElse("null"),
        Try(register(3)).getOrElse("null"),
        Try(register(4)).getOrElse("null"),
        Try(register(5)).getOrElse("null"),
        Try(register(6)).getOrElse("null"),
        Try(register(7)).getOrElse("null"),
        Try(register(8).toInt).getOrElse(0),
        Try(register(9).toInt).getOrElse(0),
        Try(register(10).toInt).getOrElse(0),
        Try(register(11).toInt).getOrElse(0),
        Try(register(12).toInt).getOrElse(0),
        Try(register(13).toInt).getOrElse(0)))
    spark.sqlContext.createDataFrame(recordsRDD)
  }

  def createDFtraficoAereoInt(records: List[List[String]]): DataFrame = {
    val recordsRDD: RDD[TraficoAereoMund] = spark.sparkContext.parallelize(records) //se convierte en RDD
      .map(register => TraficoAereoMund(
        Try(register.head).getOrElse("null"),
        Try(register(1)).getOrElse("null"), Try(register(2)).getOrElse("null"),
        Try(register(3)).getOrElse("null"), Try(register(4)).getOrElse("null"),
        Try(register(5)).getOrElse("null"), Try(register(6)).getOrElse("null"),
        Try(register(7)).getOrElse("null"), Try(register(8)).getOrElse("null"),
        Try(register(9)).getOrElse("null"), Try(register(10)).getOrElse("null"),
        Try(register(11)).getOrElse("null"), Try(register(12)).getOrElse("null"),
        Try(register(13)).getOrElse("null"), Try(register(14)).getOrElse("null"),
        Try(register(15)).getOrElse("null"), Try(register(16)).getOrElse("null"),
        Try(register(17)).getOrElse("null"), Try(register(18)).getOrElse("null"),
        Try(register(19)).getOrElse("null"), Try(register(20)).getOrElse("null"),
        Try(register(21)).getOrElse("null"), Try(register(22)).getOrElse("null"),
        Try(register(23)).getOrElse("null"), Try(register(24)).getOrElse("null"),
        Try(register(25)).getOrElse("null"), Try(register(26)).getOrElse("null"),
        Try(register(27)).getOrElse("null"), Try(register(28)).getOrElse("null"),
        Try(register(29)).getOrElse("null"), Try(register(30)).getOrElse("null"),
        Try(register(31)).getOrElse("null"), Try(register(32)).getOrElse("null")))
    spark.sqlContext.createDataFrame(recordsRDD)
  }

}
