package com.mycompany.configuration

import com.mycompany.exceptions.ConfigurationFileNotFoundException
import org.scalatest.funsuite.AnyFunSuite

class ConfigurationParametersTest extends AnyFunSuite {

  test("ifFileExists must throw ConfigurationFileNotFound exception if no path is specified"){
    assertThrows[ConfigurationFileNotFoundException]{
      ConfigurationParameters.ifFileExists("invalid/path/to/file.json")
    }
  }

}
