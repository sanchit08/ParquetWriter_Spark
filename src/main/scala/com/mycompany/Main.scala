package com.mycompany
import com.mycompany.mapper.ParquetWriter
import com.mycompany.configuration.ConfigurationParameters
import com.mycompany.exceptions.ExpectedArgumentNotFound
import com.mycompany.utils.Utils
import org.slf4j.LoggerFactory

object Main extends App {
  private val LOGGER = LoggerFactory.getLogger(Main.getClass)
  if (args.length == 0) throw ExpectedArgumentNotFound("ExpectedArgumentNotFound : first argument must be the path to the configuration file")
  ConfigurationParameters.parseConfigurationFile(args(0))
  val inputFileType = ConfigurationParameters.fileType
  val finalDF = ParquetWriter(fileType = inputFileType).finalDF
  Utils.writeToParquet(finalDF)
  LOGGER.info("Successfully written parquet file")
}