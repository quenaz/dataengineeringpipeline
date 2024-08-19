name := "dataTransformation"

version := "1.0"

scalaVersion := "2.13.14"

val sparkVersion = "3.5.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
  )

mainClass in Compile := Some("com.serasaexperian.BatchProcessingApp")