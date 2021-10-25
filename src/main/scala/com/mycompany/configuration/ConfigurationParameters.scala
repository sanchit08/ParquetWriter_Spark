package com.mycompany.configuration
import com.mycompany.exceptions.ConfigurationFileNotFoundException
import ujson.Value

import java.nio.file.{Files, Paths}
import scala.collection.mutable.ArrayBuffer


object ConfigurationParameters{

  var fileType:String=_
  var kafkaTopic:String=_
  var outputDirectory:String=_
  var kafkaURL:String=_
  var fieldData: ArrayBuffer[Field] = ArrayBuffer[Field]()

  def ifFileExists(file:String): Unit = {
    if (!Files.exists(Paths.get(file))) throw ConfigurationFileNotFoundException()
  }

  def parseConfigurationFile(file:String): Unit = {
    ifFileExists(file)
    val source = scala.io.Source.fromFile(file).mkString
    val jsonString = source.mkString
    val configurationData = ujson.read(jsonString)
    setParameters(configurationData)
  }


  def setParameters(configurationData: Value.Value): Unit ={
    this.kafkaTopic = configurationData("kafkaTopic").str
    this.fileType = configurationData("fileType").str
    this.outputDirectory = configurationData("outputDir").str
    this.kafkaURL = configurationData("kafkaURL").str
    this.fieldData = configurationData("fieldData").arr.map(i => Field(i("name").str, i("type").str, i("index").str.toInt))

  }
}
