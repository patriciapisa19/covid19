package covid19

import covid19.constants.URLSources
import org.elasticsearch.spark.sql._


object Covid19 extends App {

  val dataDF = ReadData.readINE(URLSources.HOTELCA)
  dataDF.saveToEs("spark")


}
