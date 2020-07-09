package covid19

import covid19.constants.URLSources
import covid19.utils.{CaseClassesUtil, CreateSparkSession, StringUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object ReadData {

  val spark: SparkSession = CreateSparkSession.spark

  val readSource: List[DataFrame] = URLSources.getSources map {
    case x if x.startsWith("https://ine.es") => readINE(x)
  }

  def readINE(URL: String): DataFrame = {
    val html: List[String] = scala.io.Source.fromURL(URL).mkString.split("\n").toList
    val records: List[List[String]] = html.drop(1).map(fieldName => StringUtils.normalizeString(fieldName)).map(x => x.split(";").toList)

    val recordsRDD: RDD[CaseClassesUtil.HotelesESP] = spark.sparkContext.parallelize(records) //se convierte en RDD
      .map(register => CaseClassesUtil.HotelesESP(
        Try(register.head).getOrElse("null"),
        Try(register(1)).getOrElse("null"),
        Try(register(2)).getOrElse("null"),
        Try(register(3)).getOrElse("null"),
        Try(register(4)).getOrElse("null")
      ))

    val dataDF = spark.sqlContext.createDataFrame(recordsRDD)
    dataDF

  }



}
