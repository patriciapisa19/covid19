package covid19

import java.io

import covid19.constants.Constants._
import covid19.utils.{CreateRDDUtil, ProviCAUtils}
import org.apache.spark.sql.DataFrame
import org.elasticsearch.spark.sql._
import covid19.model
import covid19.model._
import covid19.reader._
import covid19.utils.CreateRDDUtil._
import writer.WriteElastic._

import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._


object Covid19 extends App {


//  //Source: Casos Mundiales
//  val casosMundiales = ModelSource(DATOSMUNDIALESURL, CASOSMUNDFNAME, CASOSMUNDINDEX, CASOSMUNDCSV)
//  val casosMundDF: DataFrame = ReadOWIDSources.readOWID(casosMundiales.url,casosMundiales.dfName, casosMundiales.resourceCSV) //read data
//  val casosMundDF2 = CleanDataOWID.casosMundiales(casosMundDF)

//
  //Source: Movilidad Mundial
  val movilidadMundiales = ModelSource(MOVILIDADURL, MOVILIDADDFNAME, MOVILIDADINDEX, MOVILIDADCSV1)
  val movilidadMundDF: DataFrame = ReadOWIDSources.readOWID(movilidadMundiales.url,movilidadMundiales.dfName, movilidadMundiales.resourceCSV) //read data
  val movilidadMundDF2 = CleanDataOWID.casosMundiales(movilidadMundDF)



  // Source: Hoteles España
  val hotesEsp = ModelSource(HOTELURL, HOTELFNAME, HOTELESPINDEX, HOTELESESPCSV)
  val hotelesDF: DataFrame = ReadINESources.readINE(hotesEsp.url,hotesEsp.dfName, hotesEsp.resourceCSV) //read data
  //hotelesDF.show(20,false)
  val hotelesDF2: DataFrame = CleanDataINE.tipoHotelData(hotelesDF)
  hotelesDF2.printSchema()
  hotelesDF2.groupBy("ccaa").agg(count("total")).show(100,false)
  //hotelesDF2.filter(col("month") === "09").show(100,false)

  //writeES(hotelesDF2, hotesEsp.index) //load data

  //Source: Muertes España
  val muertesEsp = ModelSource(MUERTESPURL, MUERTESPNAME, MUERTESPINDEX, MUERTESPCSV)
  val muertesDF: DataFrame = ReadINESources.readINE(muertesEsp.url,muertesEsp.dfName,muertesEsp.resourceCSV) //read data
  //muertesDF.show(20,false)
  val muertesDF2: DataFrame = CleanDataINE.muertesEspData(muertesDF)
  muertesDF2.printSchema()
  muertesDF2.filter(col("month_id") === "09").show(100,false)

  //writeES(muertesDF, muertesEsp.index) //load data



  //Source: Transporte España
  val transporteEsp = ModelSource(TRANSPORTESPURL, TRANSPDFNAME, TRANSPESPINDEX, TRASPORTESPCSV)
  val transporteDF: DataFrame = ReadINESources.readINE(transporteEsp.url,transporteEsp.dfName,transporteEsp.resourceCSV) //read data
  //transporteDF.show(20,false)
  val transporteDF2: DataFrame = CleanDataINE.transporteData(transporteDF)
  transporteDF2.printSchema()
  //transporteDF2.filter(col("month") === "09").show(100,false)
  //writeES(transporteDF2, transporteEsp.index) //load data

  // Source: Casos España
  val casosEsp = ModelSource(CASOSESPURL, CASOSESPNAME, CASOSESPINDEX, CASOSESPCSV)
  val casosDF: DataFrame = ReadINESources.readINE(casosEsp.url,casosEsp.dfName, casosEsp.resourceCSV) //read data
  casosDF.show(100,false)
  val casosDF2 = CleanDataINE.casosEsp(casosDF)
  //casosDF2.show(20, false)
  //writeES(casosDF2, casosEsp.index) //load data










  //  val ineSourceData: List[(DataFrame, String)] = ReadData.readSource
//  //ineSourceData.map(x => (x._1).saveToEs(x._2)) //escribir en elastic cada df
//  //ineSourceData.map(x => x._1.show(20))
//  val hotelesDF = ineSourceData.head._1
//  val hotelIndex = ineSourceData.head._2
//  val tansporteDF = ineSourceData(2)._1
//  val transporteIndex = ineSourceData(2)._2
//
//  val hotelesCleanedDF = CleanData.hotelesData(hotelesDF)
//  val transpCleanedDF = CleanData.transporteData(tansporteDF)
//
////  hotelesCleanedDF.show(20, false)
////  transpCleanedDF.show(20, false)
//
//  val tipoHotelDF = ineSourceData(1)._1
//  val tipoHotelIndex = ineSourceData(1)._2
//  val tipohotelesCleanedDF = CleanData.tipoHotelData(tipoHotelDF)
//
////tipohotelesCleanedDF
////    .filter(col("tipo_estancia") contains  "Cam")
////    .filter(col("provincia") contains  "Astu")
////    .filter(col("year") equalTo   "2020")
////    .filter(col("month") equalTo "05")
////    .show(20, false)
//
////  hotelesCleanedDF.saveToEs(hotelIndex)
////  transpCleanedDF.saveToEs(transporteIndex)
//
//  val muertesESPDF = ineSourceData(3)._1
//  muertesESPDF.show(50,false)
//  val muertesESPIndex = ineSourceData(3)._2
//
//  val muertesDF: DataFrame = CleanData.muertesEspData(muertesESPDF)
//  muertesDF.show(50, false)
//
//  val provCADF: DataFrame = ProviCAUtils.provCADF //codigos CA y provincias
//  provCADF.show(50, false)
//
//
//  val dfToElastic: DataFrame = muertesDF.join(provCADF,"provincia")
//  dfToElastic.printSchema()
//  dfToElastic.show(50, false)
//
//  //dfToElastic.groupBy("provincia").agg(sum("total"))show(2000, false)
//  //dfToElastic.saveToEs("muertes_spain")
//
//





  }
