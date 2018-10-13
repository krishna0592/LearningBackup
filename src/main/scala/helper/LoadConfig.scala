package helper

import java.util.Properties
import scala.io.Source

object LoadConfig {
  private var properties: Properties = null

  def loadInternalConfig(filePath: String): Properties ={
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
}
