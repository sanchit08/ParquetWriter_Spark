package com.mycompany
import com.mycompany.mapper.ParquetWriter
import com.mycompany.configuration.ConfigurationParameters
import com.mycompany.exceptions.ExpectedArgumentNotFound
import org.slf4j.LoggerFactory

object Main extends App {
  private val LOGGER = LoggerFactory.getLogger(Main.getClass)
  if (args.length == 0) throw ExpectedArgumentNotFound("ExpectedArgumentNotFound : first argument must be the path to the configuration file")

  val file = args(0)
  ConfigurationParameters.parseConfigurationFile(file)
  val fileType = ConfigurationParameters.fileType
  ParquetWriter(fileType = fileType).writeToParquet()
}