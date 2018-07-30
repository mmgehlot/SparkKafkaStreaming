name := "SparkKafkaStreaming"

version := "0.1"

scalaVersion := "2.11.0"

fork := true

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.0"

// Needed for structured streams
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"

libraryDependencies += "org.apache.kafka" %% "kafka" % "0.10.0.1"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.8.0"