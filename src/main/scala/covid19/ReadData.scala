package covid19

import covid19.constants.URLSources
import covid19.utils.{CaseClassesUtil, CreateSparkSession, StringUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object ReadData {

  val spark: SparkSession = CreateSparkSession.spark
  val hotelSourceEsp: String = URLSources.HOTELESP
  val transpSourceEsp: String = URLSources.TRANSPORTESP
  val hoteldfName = "hotelesp"
  val transpdfName = "transportesp"
  val hotelEspIndex: String = "hotel-esp"
  val transpEspIndex: String = "transport-esp"

  val readSource: List[(DataFrame, String)] = URLSources.getINESource map {
    case x if x == hotelSourceEsp => readINE(x, hoteldfName)
    case x if x == transpSourceEsp => readINE(x, transpdfName)
  }

  def readINE(URL: String, dfName: String): (DataFrame, String) = {
    val html: List[String] = scala.io.Source.fromURL(URL).mkString.split("\n").toList
    val records: List[List[String]] = html.drop(1).map(fieldName => StringUtils.normalizeString(fieldName)).map(x => x.split(";").toList)

    var dataDF: DataFrame = null
    var index: String = null

    if (dfName == hoteldfName) {
      val recordsRDD: RDD[CaseClassesUtil.HotelesESP] = createRDDHotelESP(records)
      dataDF = spark.sqlContext.createDataFrame(recordsRDD)
      index = hotelEspIndex
      return (dataDF,index)
    }
    if (dfName == transpdfName) {
      val recordsRDD: RDD[CaseClassesUtil.TransporteESP] = createRDDTranspESP(records)
      dataDF = spark.sqlContext.createDataFrame(recordsRDD)
      index = transpEspIndex
      return (dataDF,index)
    }
    return (dataDF,index)

  }



  def createRDDHotelESP(records: List[List[String]] ): RDD[CaseClassesUtil.HotelesESP] = {
    val recordsRDD: RDD[CaseClassesUtil.HotelesESP] = spark.sparkContext.parallelize(records) //se convierte en RDD
      .map(register => CaseClassesUtil.HotelesESP(
        Try(register.head).getOrElse("null"),
        Try(register(1)).getOrElse("null"),
        Try(register(2)).getOrElse("null"),
        Try(register(3)).getOrElse("null"),
        Try(register(4)).getOrElse("null")
      ))
    recordsRDD
  }

  def createRDDTranspESP(records: List[List[String]]): RDD[CaseClassesUtil.TransporteESP] = {
    val recordsRDD: RDD[CaseClassesUtil.TransporteESP] = spark.sparkContext.parallelize(records) //se convierte en RDD
      .map(register => CaseClassesUtil.TransporteESP(
        Try(register.head).getOrElse("null"),
        Try(register(1)).getOrElse("null"),
        Try(register(2)).getOrElse("null"),
        Try(register(3)).getOrElse("null")
      ))
    recordsRDD
  }



}
