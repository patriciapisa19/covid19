package covid19.constants

object Constants {

  //constants source INE

  //URL
  val HOTELURL: String = "https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/2941.csv?nocab=1"
  val TRANSPORTESPURL: String = "https://ine.es/jaxiT3/files/t/es/csv_bdsc/20239.csv?nocab=1"
  val MUERTESPURL: String = "https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/36166.csv"
  val CASOSESPURL: String = "https://cnecovid.isciii.es/covid19/resources/datos_provincias.csv"



  //Index
  val HOTELESPINDEX: String = "hotel_spain"
  val TRANSPESPINDEX: String = "transport_spain"
  val MUERTESPINDEX: String = "muertes_spain"
  val CASOSESPINDEX: String = "casos_spain"


  // Dataframe Name
  val HOTELFNAME: String = "tipohotel"
  val TRANSPDFNAME: String = "transportesp"
  val MUERTESPNAME: String = "muertesp"
  val CASOSESPNAME: String = "casosesp"

  //Location csv
  val HOTELESESPCSV: String = "src/main/resources/hotelEsp.csv"
  val MUERTESPCSV: String = "src/main/resources/muertesEsp.csv"
  val TRASPORTESPCSV: String = "src/main/resources/transporteEsp.csv"
  val CASOSESPCSV: String = "src/main/resources/casosEsp.csv"
}
