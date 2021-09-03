package com.mycompany
import com.mycompany.parse.ParquetWriter
import com.mycompany.utils.Utils
import com.mycompany.configuration.ConfigurationParameters

object Main extends App {
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