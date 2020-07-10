package covid19

import covid19.constants._
import covid19.sources.ReadINESources
import covid19.utils.{CaseClassesUtil, CreateRDDUtil, CreateSparkSession, StringUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object ReadData {

  val hotelSourceEsp: String = URLSources.HOTELESP
  val transpSourceEsp: String = URLSources.TRANSPORTESP

  val readSource: List[(DataFrame, String)] = URLSources.getINESource map {
    case x if x == hotelSourceEsp => ReadINESources.readINE(x, Constants.HOTELDFNAME)
    case x if x == transpSourceEsp => ReadINESources.readINE(x, Constants.TRANSPDFNAME)
  }

}
