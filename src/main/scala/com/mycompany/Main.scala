package com.mycompany
import com.mycompany.mapper.ParquetWriter
import com.mycompany.utils.Utils
import com.mycompany.configuration.ConfigurationParameters
import com.mycompany.exceptions.ExpectedArgumentNotFound
import org.slf4j.LoggerFactory

object Main extends App {
  private val LOGGER = LoggerFactory.getLogger(Main.getClass)
  try{
    if (args.length == 0) throw ExpectedArgumentNotFound()
  }catch{
    case _: ExpectedArgumentNotFound => LOGGER.error("ExpectedArgumentNotFound : first argument must be the path to the configuration file")
      System.exit(-1)
  }
  val file = args(0)
  val confParameters = ConfigurationParameters(file)
  val fileType = confParameters.fileType
  val inputFile = confParameters.inputDirectory
  val outputDir = confParameters.outputDirectory
  val fieldData = confParameters.fieldNames
  val colNames = Utils.getColumns(fieldData.rdd)
  val parquetWriter = ParquetWriter(fileType, inputFile, outputDir, colNames)
  parquetWriter.writeToParquet()
}