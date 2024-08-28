// Define the main project
lazy val root = (project in file("."))
  .settings(
    name := "Spark Application", // Name of the main project
    scalaVersion := "2.13.14",   // Scala version
    version := "1.0",            // Project version
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.2",    // Spark Core dependency
      "org.apache.spark" %% "spark-sql" % "3.5.2",     // Spark SQL dependency
      "org.apache.hadoop" % "hadoop-common" % "3.3.2"  // Hadoop dependency
    ),
    // Specify the main class for the application
    mainClass in Compile := Some("com.serasaexperian.BatchProcessingApp"), // Main class
    // Additional settings
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8") // Java source and target compatibility
  )


// name := "dataTransformation"

// version := "1.0"

// scalaVersion := "2.13.14"

// val sparkVersion = "3.5.2"

// libraryDependencies ++= Seq(
//   "org.apache.spark" %% "spark-core" % sparkVersion,
//   "org.apache.spark" %% "spark-sql" % sparkVersion
//   )

// mainClass in Compile := Some("com.serasaexperian.BatchProcessingApp")