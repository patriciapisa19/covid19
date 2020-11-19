package covid19.utils

object CaseClassesUtil {

  //INE
  case class HotelesESP(com_aut_prov: String, viajeros_penoct: String, residencia: String, periodo: String, total: Int)

  case class TiposHotelESP(tipo_estancia: String, ccaa: String, residencia: String, viajeros_penoct: String, periodo: String, total: Int)

  case class TransporteESP(tipo_transp: String, viajeros_tasas: String, periodo: String, total: Int)

  case class MuertesESP(provincia: String, sexo: String, edad: String, tipo_dato: String, periodo: String, total: Int)

  case class CasosESP(provincia_iso: String, fecha: String, num_casos: Int, num_casos_prueba_pcr: Int,
                      num_casos_prueba_test_ac: Int, num_casos_prueba_otras: Int, num_casos_prueba_desconocida: Int)

  case class CasosMund(iso_code:String,
                       continent:String,
                       location:String,
                       date:String,
                       total_cases:String,
                       new_cases:String,
                       new_cases_smoothed:String,
                       total_deaths:String,
                       new_deaths:String,
                       new_deaths_smoothed:String,
                       total_cases_per_million:String,
                       new_cases_per_million:String,
                       new_cases_smoothed_per_million:String,
                       total_deaths_per_million:String,
                       new_deaths_per_million:String,
                       new_deaths_smoothed_per_million:String,
                       icu_patients:String,
                       icu_patients_per_million:String,
                       hosp_patients:String,
                       hosp_patients_per_million:String,
                       weekly_icu_admissions:String,
                       weekly_icu_admissions_per_million:String,
                       weekly_hosp_admissions:String,
                       weekly_hosp_admissions_per_million:String,
                       total_tests:String,
                       new_tests:String,
                       total_tests_per_thousand:String,
                       new_tests_per_thousand:String,
                       new_tests_smoothed:String,
                       new_tests_smoothed_per_thousand:String,
                       tests_per_case:String,
                       positive_rate:String,
                       tests_units:String,
                       stringency_index:String,
                       population:String,
                       population_density:String,
                       median_age:String,
                       aged_65_older:String,
                       aged_70_older:String,
                       gdp_per_capita:String,
                       extreme_poverty:String,
                       cardiovasc_death_rate:String,
                       diabetes_prevalence:String,
                       female_smokers:String,
                       male_smokers:String,
                       handwashing_facilities:String,
                       hospital_beds_per_thousand:String,
                       life_expectancy:String,
                       human_development_index:String)


  case class MovilidadMund(country_region_code: String, country_region: String, sub_region_1: String, sub_region_2: String,
                           metro_area: String, iso_3166_2_code: String, census_fips_code: String, date: String,
                           retail_and_recreation_percent_change_from_baseline: Int, grocery_and_pharmacy_percent_change_from_baseline: Int,
                           parks_percent_change_from_baseline: Int, transit_stations_percent_change_from_baseline: Int,
                           workplaces_percent_change_from_baseline: Int, residential_percent_change_from_baseline: Int)

  case class TraficoAereoMund(unit: String,tra_meas: String,tra_cov: String,schedule: String, geo_time: String,
                            Q32020: String,Q22020: String, Q12020: String,M092020: String,M082020: String,M072020: String,
                            M062020: String,M052020: String,M042020: String,M032020: String,M022020: String,
                            M012020: String,Q42019: String,Q32019: String,Q22019: String,Q12019: String,M122019: String,
                            M112019: String,M102019: String,M092019: String,M082019: String,M072019: String,M062019: String,
                            M052019: String,M042019: String,M032019: String,M022019: String,M012019: String)


}
