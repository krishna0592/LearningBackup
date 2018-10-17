package app

import org.apache.spark.sql.SparkSession
import helper.{Initialization, LoadConfig}

object MainApp extends App {

  var spark: SparkSession = _

  spark = Initialization.sparkSession()
  val prop = LoadConfig.loadPropertyOutsideJAR("/resources/config.properties")

}
