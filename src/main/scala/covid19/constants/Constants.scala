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


  //constants source our world in data

  //URL
  val DATOSMUNDIALESURL = "https://covid.ourworldindata.org/data/owid-covid-data.csv" //datos mundiales
  val MOVILIDADURL = "https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv" //datos movilidad
  //Index
  val CASOSMUNDINDEX = "casos_mundiales"
  val MOVILIDADINDEX = "movilidad_mundial"
  val TRAFICOAEREOINDEX = "trafico_aereo_int"
  // Dataframe Name
  val CASOSMUNDFNAME = "casosmund"
  val MOVILIDADDFNAME = "movilidadmund"
  val TRAFICOAEREONAME = "trafAereoIntDF"
  //Location csv
  val CASOSMUNDCSV = "src/main/resources/casosMund.csv"
  val MOVILIDADCSV1 = "src/main/resources/movilidadMund1.csv"
  val MOVILIDADCSV2 = "src/main/resources/movilidadMund2.csv"
  val TRAFICOAEREOINTCSV = "src/main/resources/trafico_aereo_internacional.csv"



}
