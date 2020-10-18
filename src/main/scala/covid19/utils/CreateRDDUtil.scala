package covid19.utils

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



}
