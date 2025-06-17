import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{
  QueryStartedEvent,
  QueryTerminatedEvent,
  QueryProgressEvent
}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.Trigger
import java.nio.file.{Files, Paths, StandardCopyOption}

import cdr.{CdrData, CdrDataError}

object ParseCDR {
  // Set up logging
  Logger.getLogger("atlas").setLevel(Level.ERROR)
  Logger.getLogger("producer").setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.WARN)
  val app_logger = Logger.getLogger("app")
  app_logger.setLevel(Level.DEBUG)

  def main(args: Array[String]): Unit = {

    val sparkBuilder = SparkSession
      .builder()
      .master("local[*]")
      .appName("CDRS data jar test")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.session.timeZone", "UTC")

    // could be run without hive for testing, export ENABLE_HIVE_SUPPORT=false
    val enableHive = sys.env.getOrElse("ENABLE_HIVE_SUPPORT", "true").toBoolean
    val spark = if (enableHive) {
      sparkBuilder.enableHiveSupport().getOrCreate()
    } else {
      sparkBuilder.getOrCreate()
    }

    // customize Spark logging
    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }
      override def onQueryTerminated(
          queryTerminated: QueryTerminatedEvent
      ): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }
      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println("Query made progress: " + queryProgress.progress)
      }
    })

    // Set up input parameters with defaults
    val inputPath = args.lift(0).getOrElse("input")
    val inputMath = args.lift(1).getOrElse("/*/*.add")
    val chkpoint = args.lift(2).getOrElse("chk-point-dir")
    val outputPath = args.lift(3).getOrElse("output")
    val outputErrPath = args.lift(4).getOrElse("outputErr")
    val processedTable = args.lift(5).getOrElse("processed")
    val maxFiles = args.lift(6).map(_.toInt).getOrElse(1)

    // import cdrdata schema
    val dataSchema = CdrData.getBaseTypes

    val rawDF = spark.readStream
      .schema(dataSchema)
      .format("csv")
      .option("sep", "|")
      .option("path", s"$inputPath$inputMath")
      .option("maxFilesPerTrigger", maxFiles) // test more options
      .load()

    app_logger.debug(s"Reading path: ${inputPath}")

    val df_filename = rawDF.withColumn("raw_filename", input_file_name())
    val df_cleanedfn = df_filename.withColumn(
      "filename",
      regexp_extract(col("filename"), ".*/(.*)", 1)
    )

    val write_parquet = df_cleanedfn.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val transformedDf = CdrData.getTransformations.foldLeft(batchDF) {
          case (tempDf, (colName, transformation)) =>
            tempDf.withColumn(colName, transformation)
        }
        val selectOrderedDF = CdrData.selectOrder(transformedDf)

        val transformErrDF = CdrDataError.getTransformations.foldLeft(batchDF) {
          case (tempDf, (colName, transformation)) =>
            tempDf.withColumn(colName, transformation)
        }

        val selectOrderErrDF = CdrDataError.selectOrder(transformErrDF)

        if (outputPath == "output") { //  TODO: BORRAR IF SOLO es para TESTsitos
          selectOrderedDF.write
            .mode("append")
            .partitionBy("billing_month", "event_hour")
            .parquet(outputPath)

          selectOrderErrDF.write
            .mode("append")
            .partitionBy("load_error_date")
            .parquet(outputErrPath)
        } else {
          selectOrderedDF.write
            .mode("append")
            .insertInto(outputPath)

          selectOrderErrDF.write
            .mode("append")
            .insertInto(outputErrPath)
        }

        app_logger.info(
          s"Writing ${filePathsToMove.length} Files processed"
        )

        // insert files to processedTable with current timestamp
        val filePathsToMove = batchDF
          .withColumnRenmed("raw_filename", "filename")
          .select("filename", current_timestamp().alias("load_date"))
          .distinct()
          .parquet(processedTable)
        


      }
      .option("checkpointLocation", chkpoint)
      .start()

    write_parquet.awaitTermination()
  }
}
