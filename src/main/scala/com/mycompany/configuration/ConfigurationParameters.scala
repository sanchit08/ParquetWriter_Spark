package com.mycompany.configuration
import com.mycompany.exceptions.ConfigurationFileNotFoundException
import org.slf4j.LoggerFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}

import java.nio.file.{Files, Paths}


object ConfigurationParameters{

  private val LOGGER = LoggerFactory.getLogger(ConfigurationParameters.getClass)
  var fileType:String=_
  var inputDirectory:String=""
  var outputDirectory:String=""
  var fieldNames:DataFrame = _

  def ifFileExists(file:String): Unit = {
    try {
      if (!Files.exists(Paths.get(file))) throw ConfigurationFileNotFoundException()
    } catch {
      case _: ConfigurationFileNotFoundException => LOGGER.error("Configuration file not found in the specified path. Please check if path is specified correctly or whether configuration file is present in given path")
        System.exit(-1)
    }
  }

  def parseConfigurationFile(file:String): DataFrame = {
    ifFileExists(file)
    val configFile: DataFrame = spark.read.option("multiline", value = true).json(file)
    LOGGER.info("Successfully parsed configuration file")
    configFile
  }

  def getParameters(file:String): Unit ={
    val configFileDF = parseConfigurationFile(file)
    this.fileType = configFileDF.select("fileType").first().getString(0)
    this.inputDirectory = configFileDF.select("inputDir").first().getString(0)
    this.outputDirectory= configFileDF.select("outputDir").first().getString(0)
    this.fieldNames = configFileDF.withColumn("field_data", explode(col("fieldNames"))).select("field_data.name", "field_data.type", "field_data.index")
  }
}
