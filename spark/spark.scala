package com.serasaexperian

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object BatchProcessingApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Batch Processing Application")
      .master("local[*]")
      .getOrCreate()

    // Path to the data lake where Parquet files are stored
    val inputPath = s"C:/Temp/serasa/project/data_lake"
    val outputPath = s"file:///C:/Temp/serasa/project/data_lake/dataTransformation/"

    // Perform batch processing
    processBatchData(spark, inputPath, outputPath)

    spark.stop()
  }

  def processBatchData(spark: SparkSession, inputPath: String, outputPath: String): Unit = {
    // Step 1: Read Parquet files from the data lake
    val rawData: DataFrame = spark.read.parquet(s"$inputPath/*/*/*")

    // Step 2: Perform data transformations
    val transformedData: DataFrame = rawData
      .filter(col("pickup_datetime").isNotNull) // Filter out rows with null pickup_datetime
      .withColumn("date", to_date(col("pickup_datetime"), "yyyy-MM-dd")) // Extract date from pickup_datetime

    // Step 3: Perform aggregations and joins based on datetime
    val aggregatedData: DataFrame = transformedData
      .groupBy("date")
      .agg(
        avg("passenger_count").as("average_value"),
        max("passenger_count").as("max_value"),
        min("passenger_count").as("min_value")
      )

    // Step 4: Write the consolidated data back to the data lake
    // Print schema and some rows to debug
    aggregatedData.printSchema()
    aggregatedData.show(5)

    // Step 3: Write the consolidated data back to the data lake
    try {
      aggregatedData.write
        .mode("overwrite")
        .parquet(outputPath)
      println(s"Data successfully written to $outputPath")
    } catch {
      case e: Exception =>
        println(s"Error writing data to $outputPath: ${e.getMessage}")
        e.printStackTrace()
    }
  }

}