package app

import java.util.Properties

import org.apache.spark.sql.SparkSession
import helper.{Initialization, LoadConfig}

object MainApp {
  var spark: SparkSession = _
  var prop: Properties = null

  def main(): Unit ={
    spark = Initialization.sparkSession()
    prop = LoadConfig.loadInternalConfig("/resources/config.properties")
    
  }
}
