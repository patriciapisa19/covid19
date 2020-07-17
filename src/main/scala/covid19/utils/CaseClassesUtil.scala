package covid19.utils

object CaseClassesUtil{

  //INE
  case class HotelesESP(com_aut_prov: String, viajeros_penoct: String, residencia: String, periodo: String, total: String)

  case class TiposHotelESP(tipo_estancia: String, com_aut_prov: String, residencia: String, viajeros_penoct: String,  periodo: String, total: String)

  case class TransporteESP(tipo_transp: String, viajeros_tasas: String, periodo: String, total: String)


}
