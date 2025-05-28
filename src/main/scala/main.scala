import org.apache.spark.sql.{DataFrame, SparkSession}
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
// import org.apache.spark.sql.hive.HiveContext

import cdr.CdrData

object ParseCDR {
  Logger.getLogger("atlas").setLevel(Level.ERROR)
  Logger.getLogger("producer").setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.WARN)
  val app_logger = Logger.getLogger("app")
  app_logger.setLevel(Level.DEBUG)

  // Function to move files
  def moveProcessedFiles(
      filePaths: Array[String],
      inputPath: String,
      processedDirectory: String
  ): Unit = {
    if (filePaths.isEmpty) {
      app_logger.info("No files to move in this batch.")
      return
    }

    filePaths.foreach { filepath =>
      val inputPathClean = inputPath.replace("file://", "")
      val processedDirectoryClean = processedDirectory.replace("file://", "")

      val originalFile = new java.io.File(s"${filepath}")
      val filename = originalFile.getName

      val destinationFile =
        new java.io.File(s"${processedDirectoryClean}/${filename}")

      if (originalFile.exists()) {
        try {
          // Ensure the processed directory exists
          val processedDir = new java.io.File(processedDirectoryClean)
          if (!processedDir.exists()) {
            processedDir.mkdirs() // Create directories if they don't exist
          }

          if (originalFile.renameTo(destinationFile)) {
            app_logger.info(
              s"Successfully moved file: ${originalFile} to ${destinationFile}"
            )
          } else {
            app_logger.error(
              s"Failed to move file: ${originalFile} to ${destinationFile}"
            )
          }
        } catch {
          case e: Exception =>
            app_logger.error(
              s"Error moving file ${originalFile}: ${e.getMessage}",
              e
            )
        }
      } else {
        app_logger.warn(
          s"File not found at original path during move attempt: ${originalFile}"
        )
      }
    }
  }

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

    val inputPath = args.lift(0).getOrElse("input")
    val chkpoint = args.lift(1).getOrElse("chk-point-dir")
    val outputPath = args.lift(2).getOrElse("output")
    val processedPath = args.lift(3).getOrElse("processed")
    val maxFiles = args.lift(4).map(_.toInt).getOrElse(1)

    val dataSchema = CdrData.getBaseTypes

    val rawDF = spark.readStream
      .schema(dataSchema)
      .format("csv")
      .option("sep", "|")
      .option("path", s"$inputPath/*/*.add")
      .option("maxFilesPerTrigger", maxFiles) // test more options
      .load()

    app_logger.debug(s"Reading path: ${inputPath}")

    val df_filename = rawDF.withColumn("raw_filename", input_file_name())
    val df_cleanedfn = df_filename.withColumn(
      "filename",
      regexp_extract(col("filename"), ".*/(.*)", 1)
    )

    val transformedDf = CdrData.getTransformations.foldLeft(df_cleanedfn) {
      case (tempDf, (colName, transformation)) =>
        tempDf.withColumn(colName, transformation)
    }

    val orderedDF = CdrData.selectOrder(transformedDf)

    // app_logger.debug(transformedDf.schema)

    // val testDF = orderedDF.select("cdr_id","cust_local_start_date", "filename")

    // val consoleDF = testDF.writeStream
    //   .outputMode("append")
    //   .format("console")
    //   .option("numRows", 2)
    //   .option("truncate", "false")
    //   .start()

    // consoleDF.awaitTermination()

    val write_parquet = orderedDF.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val excludeRawFilename = batchDF.columns.filter(_ != "raw_filename")
        val filteredDF = batchDF.select(excludeRawFilename.map(col): _*)

        // only for tests
        if (outputPath == "output") { //  TODO: BORRAR IF SOLO es para TESTsitos
          filteredDF.write
            .mode("append")
            .partitionBy("billing_month", "event_hour")
            .parquet(outputPath)
        } else {
          filteredDF.write
            .mode("append")
            .insertInto(outputPath)
        }

        val filePathsToMove = batchDF
          .select("raw_filename")
          .distinct()
          .collect()
          .map(row => s"${row.getString(0).replace("file:", "")}")

        app_logger.info(
          s"Files to consider moving for batch ${filePathsToMove.length}"
        )
        moveProcessedFiles(filePathsToMove, inputPath, processedPath)
      }
      .option("checkpointLocation", chkpoint)
      .start()

    write_parquet.awaitTermination()
  }
}
