package com.mycompany.configuration

import com.mycompany.exceptions.ConfigurationFileNotFoundException
import org.scalatest.flatspec.AnyFlatSpec
import ujson.Value

class ConfigurationParametersTest extends AnyFlatSpec {

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

}
