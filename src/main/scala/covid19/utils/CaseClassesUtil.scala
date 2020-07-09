package covid19.utils

object CaseClassesUtil{

  case class HotelesESP(com_aut_prov: String, viajeros_penoct: String, residencia: String, periodo: String, total: String)

  case class TransporteESP(tipo_transp: String, viajeros_tasas: String, periodo: String, total: String)


}
