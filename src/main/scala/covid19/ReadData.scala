package covid19

import covid19.constants.URLSources
import covid19.utils.{CreateSparkSession, StringUtils}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object ReadData {

  val spark: SparkSession = CreateSparkSession.spark

  val urlViajerosCA: String =  URLSources.VIAJEROSCA
  val html: List[String] = scala.io.Source.fromURL(urlViajerosCA).mkString.split("\n").toList
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
  print(dataDF.count())
}
