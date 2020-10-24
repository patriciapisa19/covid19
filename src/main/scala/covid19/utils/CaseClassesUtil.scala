package covid19.utils

object CaseClassesUtil{

  //INE
  case class HotelesESP(com_aut_prov: String, viajeros_penoct: String, residencia: String, periodo: String, total: Int)

  case class TiposHotelESP(tipo_estancia: String, ccaa: String, residencia: String, viajeros_penoct: String,  periodo: String, total: Int)

  case class TransporteESP(tipo_transp: String, viajeros_tasas: String, periodo: String, total: Int)

  case class MuertesESP (provincia: String, sexo: String, edad: String, tipo_dato:String, periodo: String, total: Int)

  case class CasosESP (provincia_iso: String, fecha: String, num_casos: Int, num_casos_prueba_pcr: Int,
                               num_casos_prueba_test_ac: Int, num_casos_prueba_otras: Int, num_casos_prueba_desconocida: Int)


}
