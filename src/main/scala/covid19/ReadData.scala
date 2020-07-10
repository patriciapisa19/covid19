package covid19

import covid19.constants.URLSources
import covid19.utils.{CaseClassesUtil, CreateRDDUtil, CreateSparkSession, StringUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object ReadData {

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
      dataDF = CreateRDDUtil.createDFHotelESP(records)
      index = hotelEspIndex
      return (dataDF,index)
    }
    if (dfName == transpdfName) {
      dataDF = CreateRDDUtil.createDFTranspESP(records)
      index = transpEspIndex
      return (dataDF,index)
    }
    return (dataDF,index)

  }



}
