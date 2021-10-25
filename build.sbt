name := "ParquetWriter-Spark"

version := "0.1"

scalaVersion := "2.12.10"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "3.0.0_1.0.0" % "test"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2" % "provided"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.25"
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.25" % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.2" % Test
libraryDependencies += "com.lihaoyi" %% "ujson" % "1.4.2"
libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.7.8"



