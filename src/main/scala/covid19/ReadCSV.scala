//
//
//class ReadCSV(globalConfig : Config, runConfig : Config)
//  extends BatchInterface(globalConfig : Config, runConfig : Config) with Serializable with LazyLogging {
//
//    val df = spark.read.option("delimiter", ",").option("header", true).csv("C:\\Users\\jesus.alonso\\Downloads\\ELK Masterclass\\covid19.csv")
//
////    EsSparkSQL.saveToEs(df, "coronavirus")
//
//
//
////
////    val delimiter = Try(runConfig.getString("delimiter")).getOrElse(";")
////    val header = Try(runConfig.getString("header")).getOrElse("false")
////
////    val df = sparkSession.read
////      .option("delimiter", delimiter)
////      .option("header", header)
////      .csv(runConfig.getString("file-path"))
////
////    df.toJSON.foreach(r => {
////      producer.value.sendMessageString(r, topic, key)
////    })
//
//}
