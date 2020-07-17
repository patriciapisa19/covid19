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
  val hotelIndex = ineSourceData.head._2
  val tansporteDF = ineSourceData(1)._1
  val transporteIndex = ineSourceData(1)._2

  val hotelesCleanedDF = CleanData.hotelesData(hotelesDF)
  val transpCleanedDF = CleanData.transporteData(tansporteDF)
  //hotelesCleanedDF.saveToEs("hotelIndex")

  hotelesCleanedDF.show(20, false)
  transpCleanedDF.show(20, false)




  }
