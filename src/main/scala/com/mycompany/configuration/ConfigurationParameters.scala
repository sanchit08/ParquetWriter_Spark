package com.mycompany.configuration

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}

case class ConfigurationParameters(file:String){
  private val configFile: DataFrame = spark.read.option("multiline",value = true).json(file)
  val fileType: String = configFile.select("fileType").first().getString(0)
  val inputDirectory: String = configFile.select("inputDir").first().getString(0)
  val outputDirectory: String = configFile.select("outputDir").first().getString(0)
  val fieldNames: DataFrame = configFile.withColumn("field_data",explode(col("fieldNames"))).select("field_data.name","field_data.type","field_data.index")
}
