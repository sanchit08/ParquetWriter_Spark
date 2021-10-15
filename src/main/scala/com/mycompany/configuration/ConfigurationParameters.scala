package com.mycompany.configuration
import com.mycompany.exceptions.ConfigurationFileNotFoundException
import org.slf4j.LoggerFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import ujson.Value

import java.nio.file.{Files, Paths}
import scala.collection.mutable.ArrayBuffer


object ConfigurationParameters{

  var fileType:String=_
  var kafkaTopic:String=""
  var outputDirectory:String=""
  var fieldData: ArrayBuffer[Field] = ArrayBuffer[Field]()

  def ifFileExists(file:String): Unit = {
    if (!Files.exists(Paths.get(file))) throw ConfigurationFileNotFoundException()
  }

  def parseConfigurationFile(file:String): Unit = {
    ifFileExists(file)
    val jsonString = scala.io.Source.fromFile(file).mkString
    val configurationData = ujson.read(jsonString)
    setParameters(configurationData)

  }

  def setParameters(configurationData: Value.Value): Unit ={
    this.kafkaTopic = configurationData("kafkaTopic").str
    this.fileType = configurationData("fileType").str
    this.outputDirectory = configurationData("outputDir").str

    for (data <- configurationData("fieldData").arr){
      this.fieldData += Field(fieldName = data("name").str, fieldType = data("type").str, index = data("index").str.toInt)
    }
  }
}
