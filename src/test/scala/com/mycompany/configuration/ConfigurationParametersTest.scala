package com.mycompany.configuration

import com.mycompany.exceptions.ConfigurationFileNotFoundException
import org.scalatest.FlatSpec
import ujson.Value

import scala.collection.mutable.ArrayBuffer

class ConfigurationParametersTest extends FlatSpec {

  def fixtures =
    new {
      val jsonString = scala.io.Source.fromResource("conf.json").mkString
      val configurationData = ujson.read(jsonString)
    }

  "ifFileExists" should "throw ConfigurationFileNotFoundException" in {
    assertThrows[ConfigurationFileNotFoundException]{
      ConfigurationParameters.ifFileExists("/invalid/file/path")
    }
  }

  "parseConfigurationFile" should "successfully parse configuration file" in{
    val f = fixtures
    assert(f.configurationData.isInstanceOf[Value.Value])
  }

  "setParameters" should "set values from configuration file to ConfigurationParameters" in {
    val f = fixtures
    ConfigurationParameters.setParameters(f.configurationData)
    assert(ConfigurationParameters.fileType === "csv")
    assert(ConfigurationParameters.kafkaTopic === "http")
    assert(ConfigurationParameters.outputDirectory === "output")
    assert(ConfigurationParameters.fieldData(0) === Field(fieldName = "ts", fieldType = "double", index = 0))

  }

  it should "extract the fields with correct data type" in {
    val f = fixtures
    ConfigurationParameters.setParameters(f.configurationData)
    assert(ConfigurationParameters.fileType.isInstanceOf[String])
    assert(ConfigurationParameters.fieldData.isInstanceOf[ArrayBuffer[Field]])
  }
}
