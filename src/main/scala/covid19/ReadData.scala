package covid19

import covid19.constants._
import covid19.sources.ReadINESources
import covid19.utils.{CaseClassesUtil, CreateRDDUtil, CreateSparkSession, StringUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object ReadData {

  val hotelSourceEsp: String = URLSources.TURISMOESP
  val transpSourceEsp: String = URLSources.TRANSPORTESP
  val tipoHotelSource = URLSources.TIPOSHOTEL

  val readSource: List[(DataFrame, String)] = URLSources.getINESource map {
    case x if x == hotelSourceEsp => ReadINESources.readINE(x, Constants.TURISMODFNAME)
    case x if x == tipoHotelSource => ReadINESources.readINE(x, Constants.TIPOTURISMODFNAME)
    case x if x == transpSourceEsp => ReadINESources.readINE(x, Constants.TRANSPDFNAME)
  }

}
