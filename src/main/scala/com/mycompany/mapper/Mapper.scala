package com.mycompany.mapper
import com.mycompany.configuration.{ConfigurationParameters, Field}
import com.mycompany.utils.Utils
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait Mapper {
  val outputDir: String = ConfigurationParameters.outputDirectory
  val kafkaTopic: String = ConfigurationParameters.kafkaTopic
  val fieldData: ArrayBuffer[Field] = ConfigurationParameters.fieldData
  val df: DataFrame = spark.read.format("kafka")
    .option("kafka.bootstrap.servers","192.168.32.3:9093")
    .option("subscribe",kafkaTopic).load()
  var finalDF: DataFrame

  def writeToParquet(): Unit = {
    finalDF.write.parquet("hdfs://namenode:9000/"+outputDir)
  }
}

object ParquetWriter{
  private val LOGGER = LoggerFactory.getLogger(ParquetWriter.getClass)
  def apply(fileType:String): Mapper = fileType match{
    case "csv" =>CSVMapper()
    case "tsv" => TSVMapper()
    case "json" => JSONMapper()
  }
  LOGGER.info("Successfully written parquet file")
}