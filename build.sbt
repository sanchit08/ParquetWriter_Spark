name := "ParquetWriter_Spark"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2" % "provided"
libraryDependencies += "org.slf4j" % "slf4j-api" % "2.0.0-alpha1"
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "2.0.0-alpha1" % Test


