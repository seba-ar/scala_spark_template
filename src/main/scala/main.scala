import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

object Example extends Serializable { // Edit the Example with the name of the app
  var spark_logger = Logger.getLogger("org.apache.spark")
  var app_logger = Logger.getLogger("app")
  spark_logger.setLevel(Level.WARN)
  app_logger.setLevel(Level.DEBUG)

    
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Example")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions", 3)
      .getOrCreate()

    app_logger.debug("hola desde debug")
    app_logger.info("hola desde info")
  }
}