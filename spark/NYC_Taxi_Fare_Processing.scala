import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object NYC_Taxi_Fare_Processing {

  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("NYC Taxi Fare Processing")
      .master("local[*]")
      .getOrCreate()

    // Define the data lake path
    val dataLakePath = "data_lake/nyc_taxi_fares"

    // Read Parquet files from the data lake
    val taxiFaresDF = spark.read.parquet(s"$dataLakePath/*/*/*/")

    // Transformation: Calculate the average fare amount per place (borough)
    val avgFareByPlaceDF = taxiFaresDF
      .groupBy("place")
      .agg(avg("fare_amount").alias("avg_fare"))

    // Write the processed data back to the data lake
    avgFareByPlaceDF.write
      .mode("overwrite")
      .parquet("data_lake/nyc_taxi_fares_summary/avg_fare_by_place")

    // Stop the Spark session
    spark.stop()
  }
}
