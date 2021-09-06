package com.mycompany.configuration
import com.mycompany.exceptions.ConfigurationFileNotFound
import org.slf4j.LoggerFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}

import java.nio.file.{Files, Paths}


case class ConfigurationParameters(file:String){
  private val LOGGER = LoggerFactory.getLogger(ConfigurationParameters.getClass)
  try{
    if (!Files.exists(Paths.get(file))) throw ConfigurationFileNotFound()
  }catch {
    case _: ConfigurationFileNotFound => LOGGER.error("Configuration file not found in the specified path. Please check if path is specified correctly or whether configuration file is present in given path")
      System.exit(-1)
  }
  private val configFile: DataFrame = spark.read.option("multiline",value = true).json(file)
  val fileType: String = configFile.select("fileType").first().getString(0)
  val inputDirectory: String = configFile.select("inputDir").first().getString(0)
  val outputDirectory: String = configFile.select("outputDir").first().getString(0)
  val fieldNames: DataFrame = configFile.withColumn("field_data",explode(col("fieldNames"))).select("field_data.name","field_data.type","field_data.index")
  LOGGER.info("Successfully parsed configuration file")
}
