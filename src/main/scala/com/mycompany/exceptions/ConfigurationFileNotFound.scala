package com.mycompany.exceptions

case class ConfigurationFileNotFoundException(message: String="") extends Exception(message)
