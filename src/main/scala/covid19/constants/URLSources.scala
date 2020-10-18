package covid19.constants

object URLSources {
  //Source: INE

  //Dataset: Viajeros y pernoctaciones por comunidades autónomas y provincias
  //val TURISMOESP = "https://ine.es/jaxiT3/files/t/es/csv_bdsc/2074.csv"

  //Dataset: Tipos de hoteles
  val HOTELURL = "https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/2941.csv?nocab=1"

  //Dataset: Total de viajeros por tipo, medio de transporte (terrestre, aéreo y maritimo) y distancia
  val TRANSPORTESPURL = "https://ine.es/jaxiT3/files/t/es/csv_bdsc/20239.csv?nocab=1"

  val MUERTESPURL = "https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/36166.csv"

  val getINESource: List[String] = List(HOTELURL,TRANSPORTESPURL,MUERTESPURL)

  //Source: ??
  val CASOSPROV = "http://cnecovid.isciii.es/covid19/resources/datos_provincias.csv"
}

