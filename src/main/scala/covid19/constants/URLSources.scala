package covid19.constants

object URLSources {
  //Source: INE

  //Dataset: Viajeros y pernoctaciones por comunidades autónomas y provincias
  val HOTELESP = "https://ine.es/jaxiT3/files/t/es/csv_bdsc/2074.csv"

  //Dataset: Total de viajeros por tipo, medio de transporte (terrestre, aéreo y maritimo) y distancia
  val TRANSPORTESP = "https://ine.es/jaxiT3/files/t/es/csv_bdsc/20239.csv?nocab=1"

  val getINESource: List[String] = List(HOTELESP,TRANSPORTESP)

}

