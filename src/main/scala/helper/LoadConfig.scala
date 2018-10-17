package helper

import java.util.Properties
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkFiles

import scala.io.Source

object LoadConfig {
  private var properties: Properties = null
  private var externalConfig: Config = null

  def loadPropertyWithinJAR(filePath: String): Properties ={
    properties = new Properties()
    val url = getClass.getResource(filePath)
    if(url != null){
      val source = Source.fromURL(url)
      properties.load(source.bufferedReader())
      source.close()
    }else{
      println("There is no file in the location for DP jar " + filePath)
    }
    properties
  }


  def loadPropertyOutsideJAR(fileName: String): Config ={
    /*
      while using this method pass the config file in spark-submit in --files parameter
      for eg:
        spark-submit --num-executors 2 --class myapp.MainApp --files /home/localConfig.properties,/home/childConfig.properties /home/LearningBackup-assembly-0.1.jar localConfig.properties childConfig.properties
        in main method access it as
          val prop = LoadConfig.loadPropertyOutsideJAR(args(0))
     */
    val fileName: String = SparkFiles.get(fileName)
    externalConfig = ConfigFactory.load(ConfigFactory.parseFile(new java.io.File(fileName)))
    externalConfig
  }
}
