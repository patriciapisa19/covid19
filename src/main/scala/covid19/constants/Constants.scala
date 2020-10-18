package covid19.constants

object Constants {

  //constants source INE

  //URL
  val HOTELURL = "https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/2941.csv?nocab=1"
  val TRANSPORTESPURL = "https://ine.es/jaxiT3/files/t/es/csv_bdsc/20239.csv?nocab=1"
  val MUERTESPURL = "https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/36166.csv"
  val CASOSPROV = "http://cnecovid.isciii.es/covid19/resources/datos_provincias.csv"


  //Index
  val HOTELESPINDEX: String = "hotel-esp"
  val TRANSPESPINDEX: String = "transport-esp"
  val MUERTESPINDEX = "muertes-esp"

  // Dataframe Name
  val HOTELFNAME = "tipohotel"
  val TRANSPDFNAME = "transportesp"
  val MUERTESPNAME = "muertesp"

  //Location csv
  val HOTELESESPCSV = "src/main/resources/hotelEsp.csv"
  val MUERTESPCSV = "src/main/resources/muertesEsp.csv"
  val TRASPORTESPCSV = "src/main/resources/transporteEsp.csv"
}
