package covid19.sources

import covid19.constants._
import covid19.utils.{CreateRDDUtil, StringUtils}
import org.apache.spark.sql.DataFrame

object ReadINESources {



  def readINE(URL: String, dfName: String): (DataFrame, String) = {
    val html: List[String] = scala.io.Source.fromURL(URL).mkString.split("\n").toList
    val records: List[List[String]] = html.drop(1).map(fieldName => StringUtils.normalizeString(fieldName)).map(x => x.split(";").toList)

    var dataDF: DataFrame = null
    var index: String = null

    if (dfName == Constants.TURISMODFNAME) {
      dataDF = CreateRDDUtil.createDFHotelESP(records)
      index = Constants.TURIMOESPINDEX
      return (dataDF,index)
    }
    if (dfName == Constants.TIPOTURISMODFNAME) {
      dataDF = CreateRDDUtil.createDFTipoHotelESP(records)
      return (dataDF,index)
    }
    if (dfName == Constants.TRANSPDFNAME) {
      dataDF = CreateRDDUtil.createDFTranspESP(records)
      index = Constants.TRANSPESPINDEX
      return (dataDF,index)
    }
    if (dfName == Constants.MUERTESPNAME) {
      dataDF = CreateRDDUtil.createDFMuertesESP(records)
      index = Constants.MUERTESPINDEX
      return (dataDF,index)
    }
    return (dataDF,index)

  }
}
