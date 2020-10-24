package covid19.reader

import covid19.constants._
import covid19.utils.CreateRDDUtil._
import covid19.utils._
import javax.net.ssl.SSLHandshakeException
import org.apache.spark.sql.DataFrame

import scala.util.control.Exception

object ReadINESources {

  def readINE(URL: String, dfName: String, resource_csv: String): DataFrame = {

    var dataDF: DataFrame = null

    try {
      val html: List[String] = scala.io.Source.fromURL(URL).mkString.split("\n").toList
      val records: List[List[String]] = html.drop(1).map(fieldName => StringUtils.normalizeString(fieldName)).map(x => x.split(",").toList)
      dataDF = dfName match {
        case Constants.HOTELFNAME => createDFTipoHotelESP(records)
        case Constants.TRANSPDFNAME => createDFTranspESP(records)
        case Constants.MUERTESPNAME => createDFMuertesESP(records)
        case Constants.CASOSESPNAME => createDFCasosESP(records)

      }
      dataDF.show(20,false)
    }
    catch {
      case e: SSLHandshakeException => {
        println("!!!!!!!!!!!!!!!!!!!!!!error pkix!!!!!!!!!!!!!!1111" + e)
        dataDF = spark.read.format("csv").option("sep", ";").option("header", "true").load(resource_csv)
        dataDF.show(20,false)
      }
    }
    dataDF






//    if (dfName == Constants.TIPOTURISMODFNAME) {
//      dataDF = CreateRDDUtil.createDFTipoHotelESP(records)
//      return (dataDF,index)
//    }
//    if (dfName == Constants.TRANSPDFNAME) {
//      dataDF = CreateRDDUtil.createDFTranspESP(records)
//      index = Constants.TRANSPESPINDEX
//      return (dataDF,index)
//    }
//    if (dfName == Constants.MUERTESPNAME) {
//      dataDF = CreateRDDUtil.createDFMuertesESP(records)
//      index = Constants.MUERTESPINDEX
//      return (dataDF,index)
//    }

  }
}
