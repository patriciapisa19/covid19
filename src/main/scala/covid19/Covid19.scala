package covid19

import java.io

import covid19.constants.URLSources
import org.apache.spark.sql.DataFrame
import org.elasticsearch.spark.sql._

import scala.collection.JavaConverters._


object Covid19 extends App {

  val ineSourceData: List[(DataFrame, String)] = ReadData.readSource
  //ineSourceData.map(x => (x._1).saveToEs(x._2)) //escribir en elastic cada df
  //ineSourceData.map(x => x._1.show(20))
  val hotelesDF = ineSourceData.head._1
  val tansporteDF = ineSourceData(1)._1

  val hotelesCleanedDF = CleanData.hotelesData(hotelesDF)





  }
