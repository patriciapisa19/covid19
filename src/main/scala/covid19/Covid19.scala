package covid19
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

import scala.collection.immutable



object Covid19 extends App {

  //lECTURA DEL DATASET QUE ESTA EN ESTA RUTA
  val url: String =  "https://ine.es/jaxiT3/files/t/es/csv_bdsc/2074.csv"
  val html: List[String] = scala.io.Source.fromURL(url).mkString.split("\n").toList

  val records: List[Row] = html.drop(1).map(fieldName => if(fieldName.contains("\r")) fieldName.dropRight(1) else fieldName)
    .map(x => x.split(";").toList)
    .map(x => Row(x:_*))

  val headInit: List[String] = html(0).split(";").toList
  val head = headInit.map(fieldName => if(fieldName.contains("\r")) fieldName.dropRight(1) else fieldName)
  val headStruct: List[StructField] = head.map(fieldName => StructField(fieldName, StringType, nullable = true))

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
    .getOrCreate()

  import spark.sqlContext.implicits._

  val recordsRDD = spark.sparkContext.parallelize(records) //se convierte en RDD
  val dataDF = spark.createDataFrame(recordsRDD, StructType(headStruct))
  dataDF.show(20, truncate = false)
  print(dataDF.count())






  //  val array = Array.fill(2,2)("A")
//
//  println(array.deep)
//  println(array.deep.mkString("\n"))
//





}
