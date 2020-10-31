package covid19.reader

import covid19.utils.CreateRDDUtil._
import org.apache.spark.sql.DataFrame
import covid19.constants.Constants._
import covid19.utils.StringUtils
import javax.net.ssl.SSLHandshakeException

object ReadOWIDSources {

  def readOWID(URL: String, dfName: String, resource_csv: String): DataFrame = {

    var dataDF: DataFrame = null

    if (dfName == MOVILIDADDFNAME) {
      val dataDF1 = spark.read.format("csv").option("sep", ",").option("header", "true").load(MOVILIDADCSV1)
      val dataDF2 = spark.read.format("csv").option("sep", ",").option("header", "true").load(MOVILIDADCSV2)
      dataDF = dataDF1.union(dataDF2)
      dataDF.show(20, false)

    }

    if (dfName == TRAFICOAEREONAME) {
      dataDF = spark.read.format("csv").option("sep", ",").option("header", "true").load(TRAFICOAEREOINTCSV)
      dataDF.show(20, false)

    }

    if (dfName == CASOSMUNDFNAME){
      try {
        val html: List[String] = scala.io.Source.fromURL (URL).mkString.split ("\n").toList
        val records: List[List[String]] = html.drop(1).map(fieldName => StringUtils.normalizeString(fieldName)).map(x => x.split(",").toList)
        dataDF = createDFCasosMund(records)
        dataDF.printSchema()
        dataDF.show(300,false)
        }
      catch {
        case e: SSLHandshakeException => {
          println ("!!!!!!!!!!!!!!!!!!!!!!error pkix!!!!!!!!!!!!!!1111" + e)
          dataDF = spark.read.format ("csv").option ("sep", ",").option ("header", "true").load (resource_csv)
          dataDF.show (200, false)
        }
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
