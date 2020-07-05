package covid19

import covid19.constants.URLSources
import covid19.utils.{CreateSparkSession, StringUtils}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object ReadData {

  val spark: SparkSession = CreateSparkSession.spark

  val readSource: List[DataFrame] = URLSources.getSources map {
    case x if x.startsWith("https://ine.es") => readINE(x)
  }


  def readINE (URL: String) : DataFrame = {
    val html: List[String] = scala.io.Source.fromURL(URL).mkString.split("\n").toList
    val records: List[Row] = html.drop(1).map(fieldName => StringUtils.normalizeString(fieldName))
      .map(x => x.split(";").toList)
      .map(x => Row(x:_*))

    val headInit: List[String] = html(0).split(";").toList
    val head = headInit.map(fieldName => StringUtils.normalizeString(fieldName))
    val headStruct: List[StructField] = head.map(fieldName => StructField(fieldName, StringType, nullable = true))


    val recordsRDD = spark.sparkContext.parallelize(records) //se convierte en RDD
    val dataDF = spark.createDataFrame(recordsRDD, StructType(headStruct))
    dataDF.show(20, truncate = false)
    dataDF
  }


}
