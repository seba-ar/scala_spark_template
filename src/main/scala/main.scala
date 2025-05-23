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

import cdr.CdrData

object ParseCDR extends Serializable {
  Logger.getLogger("atlas").setLevel(Level.ERROR)
  val spark_logger = Logger.getLogger("org")
  val app_logger = Logger.getLogger("app")
  spark_logger.setLevel(Level.WARN)
  app_logger.setLevel(Level.DEBUG)

  // Function to move files
  def moveProcessedFiles(
      filePaths: Array[String],
      processedDirectory: String
  ): Unit = {
    if (filePaths.isEmpty) {
      app_logger.info("No files to move in this batch.")
      return
    }

    filePaths.foreach { filePath =>
      val originalFile = new java.io.File(filePath)
      val fileName = originalFile.getName
      val destinationFile =
        new java.io.File(s"${processedDirectory}/${fileName}")

      if (originalFile.exists()) {
        try {
          // Ensure the processed directory exists
          val processedDir = new java.io.File(processedDirectory)
          if (!processedDir.exists()) {
            processedDir.mkdirs() // Create directories if they don't exist
          }

          if (originalFile.renameTo(destinationFile)) {
            // app_logger.info(
            //   s"Successfully moved file: ${filePath} to ${destinationFile.getAbsolutePath}"
            // )
          } else {
            app_logger.error(
              s"Failed to move file: ${filePath} to ${destinationFile.getAbsolutePath}"
            )
          }
        } catch {
          case e: Exception =>
            app_logger.error(
              s"Error moving file ${filePath}: ${e.getMessage}",
              e
            )
        }
      } else {
        app_logger.warn(
          s"File not found at original path during move attempt: ${filePath}"
        )
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("CDRS jar test")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()
    
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

    var inputPath = ""
    if (args.length < 1) {
      // app_logger.debug("Usage: ParseCDR <input-path>")
      inputPath = "input/*.add"
      // System.exit(1)
    } else {
      inputPath = args(0)
    }

    var chkpoint = "chk-point-dir"

    if (args.length > 1) {
      chkpoint = args(1)
    }

    val dataSchema = CdrData.getBaseTypes

    val processedPath = "processed"
    var outputPath = "output"

    if (args.length > 2) {
      outputPath = args(2)
    }

    val rawDF = spark.readStream
      .schema(dataSchema)
      .format("csv")
      .option("sep", "|")
      .option("path", inputPath)
      .option("maxFilesPerTrigger", 1)
      .load()

    app_logger.debug(s"starting read path: ${inputPath}")

    val df_filename = rawDF.withColumn("filename", input_file_name())

    val transformedDf = CdrData.getTransformations.foldLeft(df_filename) {
      case (tempDf, (colName, transformation)) =>
        tempDf.withColumn(colName, transformation)
    }

    val orderedDF = CdrData.selectOrder(transformedDf)

    // app_logger.debug(transformedDf.schema)

    // val consoleDF = transformedDf.writeStream
    //   .outputMode("append")
    //   .format("console")
    //   .option("numRows", 2)
    //   .start()

    // consoleDF.awaitTermination()

    val write_parquet = orderedDF.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        batchDF.write
          .mode("append")
          .partitionBy("billing_month", "event_hour")
          .parquet(outputPath)

        // val filePathsToMove = batchDF
        //   .select("filename")
        //   .distinct()
        //   .collect().map(row => row.getString(0).replace("file:", ""))

        // app_logger.info(
        //   s"Files to consider moving for batch ${filePathsToMove.length}"
        // )
        // moveProcessedFiles(filePathsToMove, processedPath)

      }
      // .option("checkpointLocation", chkpoint)
      // .trigger(Trigger.ProcessingTime("20 seconds"))
      .start()

    write_parquet.awaitTermination()

  }
}
