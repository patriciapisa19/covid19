package covid19

import java.io

import covid19.constants.URLSources
import covid19.utils.ProviCAUtils
import org.apache.spark.sql.DataFrame
import org.elasticsearch.spark.sql._

import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._


object Covid19 extends App {

  val ineSourceData: List[(DataFrame, String)] = ReadData.readSource
  //ineSourceData.map(x => (x._1).saveToEs(x._2)) //escribir en elastic cada df
  //ineSourceData.map(x => x._1.show(20))
  val hotelesDF = ineSourceData.head._1
  val hotelIndex = ineSourceData.head._2
  val tansporteDF = ineSourceData(2)._1
  val transporteIndex = ineSourceData(2)._2

  val hotelesCleanedDF = CleanData.hotelesData(hotelesDF)
  val transpCleanedDF = CleanData.transporteData(tansporteDF)

//  hotelesCleanedDF.show(20, false)
//  transpCleanedDF.show(20, false)

  val tipoHotelDF = ineSourceData(1)._1
  val tipoHotelIndex = ineSourceData(1)._2
  val tipohotelesCleanedDF = CleanData.tipoHotelData(tipoHotelDF)

//tipohotelesCleanedDF
//    .filter(col("tipo_estancia") contains  "Cam")
//    .filter(col("provincia") contains  "Astu")
//    .filter(col("year") equalTo   "2020")
//    .filter(col("month") equalTo "05")
//    .show(20, false)

//  hotelesCleanedDF.saveToEs(hotelIndex)
//  transpCleanedDF.saveToEs(transporteIndex)

  val muertesESPDF = ineSourceData(3)._1
  val muertesESPIndex = ineSourceData(3)._2

  val muertesDF: DataFrame = CleanData.muertesEspData(muertesESPDF)
  muertesDF.show(50, false)

  val provCADF: DataFrame = ProviCAUtils.provCADF //codigos CA y provincias
  provCADF.show(50, false)


  muertesDF.join(provCADF,"provincia").show(20,false)







  }
