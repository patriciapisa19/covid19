package covid19.utils

object CaseClassesUtil{

  //INE
  case class HotelesESP(com_aut_prov: String, viajeros_penoct: String, residencia: String, periodo: String, total: Int)

  case class TiposHotelESP(tipo_estancia: String, ccaa: String, residencia: String, viajeros_penoct: String,  periodo: String, total: Int)

  case class TransporteESP(tipo_transp: String, viajeros_tasas: String, periodo: String, total: Int)

  case class MuertesESP (provincia: String, sexo: String, edad: String, tipo_dato:String, periodo: String, total: Int)

  case class CasosESP (provincia_iso: String, fecha: String, num_casos: Int, num_casos_prueba_pcr: Int,
                               num_casos_prueba_test_ac: Int, num_casos_prueba_otras: Int, num_casos_prueba_desconocida: Int)

  case class CasosMund (iso_code : String, continent : String, location : String, date : String, total_cases : String,
                        new_cases : String, new_cases_smoothed : String, total_deaths : String, new_deaths : String,
                        new_deaths_smoothed : String, total_cases_per_million : String, new_cases_per_million : String,
                        new_cases_smoothed_per_million : String, total_deaths_per_million : String, new_deaths_per_million : String,
                        new_deaths_smoothed_per_million : String, total_tests : String, new_tests : String, total_tests_per_thousand : String,
                        new_tests_per_thousand : String, new_tests_smoothed : String, new_tests_smoothed_per_thousand : String,
                        tests_per_case : String, positive_rate : String, tests_units : String, stringency_index : String, population : String,
                        population_density : String, median_age : String, aged_65_older : String, aged_70_older : String, gdp_per_capita : String,
                        extreme_poverty : String, cardiovasc_death_rate : String, diabetes_prevalence : String, female_smokers : String,
                        male_smokers : String, handwashing_facilities : String, hospital_beds_per_thousand : String, life_expectancy : String,
                        human_development_index: String)

  case class MovilidadMund (country_region_code: String, country_region: String, sub_region_1: String, sub_region_2: String,
                            metro_area: String, iso_3166_2_code: String, census_fips_code: String, date: String,
                            retail_and_recreation_percent_change_from_baseline: Int, grocery_and_pharmacy_percent_change_from_baseline: Int,
                            parks_percent_change_from_baseline: Int, transit_stations_percent_change_from_baseline: Int,
                            workplaces_percent_change_from_baseline: Int, residential_percent_change_from_baseline: Int)


}
