package covid19.utils

object StringUtils {

  def normalizeString (field : String): String = if(field.contains("\r")) field.dropRight(1) else field
}
