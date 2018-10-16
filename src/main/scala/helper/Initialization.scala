package helper

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Initialization {
  private var spark_session: SparkSession = _

  def sparkSession(): SparkSession ={
    spark_session = SparkSession
      .builder()
      .appName("LearningBackups")
      //.enableHiveSupport()
      .getOrCreate()

    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    spark_session
  }
}
