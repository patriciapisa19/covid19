package writer

import org.apache.spark.sql.DataFrame
import org.elasticsearch.spark.sql._

object WriteElastic {

  def writeES (df: DataFrame, index: String): Unit = {
    df.saveToEs(index)
  }

}
