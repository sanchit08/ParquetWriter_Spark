package com.mycompany.mapper
import com.mycompany.configuration.ConfigurationParameters
import com.mycompany.utils.Utils
import org.apache.spark.sql.DataFrame

import org.slf4j.LoggerFactory
import scala.collection.mutable

trait Mapper {
  val outputDir: String = ConfigurationParameters.outputDirectory
  val inputFile: String = ConfigurationParameters.inputDirectory
  val fieldNames: DataFrame = ConfigurationParameters.fieldNames
  val columnNames: mutable.Map[String, String] = Utils.getColumns(fieldNames.rdd)
  val colNames: Array[String] = columnNames.keySet.toArray
  var df: DataFrame

  def writeToParquet(): Unit = {
    df.write.parquet(outputDir)
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