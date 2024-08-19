import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object BatchProcessingApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Batch Processing Application")
      .master("local[*]")
      .getOrCreate()

    // Path to the data lake where Parquet files are stored
    val inputPath = "file:///C:/Temp/serasa/project/data_lake/data_0_to_999.parquet"
    val outputPath = "file:///C:/Temp/serasa/project/data_lake/dataTransformation/"

    // Perform batch processing
    processBatchData(spark, inputPath, outputPath)

    spark.stop()
  }

  def processBatchData(spark: SparkSession, inputPath: String, outputPath: String): Unit = {
    // Step 1: Read Parquet files from the data lake
    val rawData: DataFrame = spark.read.parquet(inputPath)

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
    // aggregatedData.write.mode("overwrite").parquet(outputPath)
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


// import org.apache.spark.sql.{SparkSession, DataFrame}
// import org.apache.spark.sql.functions._

// object NYC_Taxi_Fare_Processing {

//   def main(args: Array[String]): Unit = {
//     // Initialize SparkSession
//     val spark = SparkSession.builder()
//       .appName("NYC Taxi Fare Processing")
//       .config("spark.sql.warehouse.dir", "C:\\Temp\\Spark")
//       .master("local[*]")
//       .getOrCreate()

//     // Define the data lake path
//     val dataLakePath = "C:\\Temp\\serasa\\project\\data_lake"

//     // Read Parquet files from the data lake
//     val taxiFaresDF = spark.read.parquet(s"$dataLakePath/*/*/*/")

//     // Transformation: Calculate the average fare amount per place (borough)
//     val avgFareByPlaceDF = taxiFaresDF
//       .groupBy("place")
//       .agg(avg("fare_amount").alias("avg_fare"))

//     // Write the processed data back to the data lake
//     avgFareByPlaceDF.write
//       .mode("overwrite")
//       .parquet("data_lake/nyc_taxi_fares_summary/avg_fare_by_place")

//     // Stop the Spark session
//     spark.stop()
//   }
// }
