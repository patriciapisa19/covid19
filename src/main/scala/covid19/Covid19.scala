package covid19

import java.io

import covid19.constants.URLSources
import org.apache.spark.sql.DataFrame
import org.elasticsearch.spark.sql._

import scala.collection.JavaConverters._


object Covid19 extends App {

//  val ineSourceData: List[List[io.Serializable]] = {
//    val df: List[DataFrame] = ReadData.readSource.map(x => x._1)
//    val index: List[String] = ReadData.readSource.map(x => x._2)
//    List(df,index)
//  }

  val ineSourceData: List[(DataFrame, String)] = ReadData.readSource
  ineSourceData.map(x => (x._1).show(20))
  ineSourceData.map(x => (x._1).saveToEs(x._2))

//  val df1: DataFrame = ineSourceData.head._1
//  df1.show(20)
//  val index1: String = ineSourceData.head._2






  //ineSourceData.map()

}
