package covid19

import java.io

import covid19.constants.URLSources
import org.apache.spark.sql.DataFrame
import org.elasticsearch.spark.sql._

import scala.collection.JavaConverters._


object Covid19 extends App {

  val ineSourceData: List[(DataFrame, String)] = ReadData.readSource
  ineSourceData.map(x => (x._1).saveToEs(x._2)) //escribir en elastic cada df
}
