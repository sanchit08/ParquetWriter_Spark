package com.mycompany.utils

import com.mycompany.configuration.{ConfigurationParameters, Field}
import org.apache.spark.sql.functions.{col, from_json, split}
import org.apache.spark.sql.types.{MapType, StringType}
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable.ArrayBuffer

object Utils{
  def writeToParquet(finalDF: DataFrame): Unit = {
    finalDF.write.mode(SaveMode.Overwrite).parquet(ConfigurationParameters.hdfsPath+ConfigurationParameters.outputDirectory)
  }

  def getSplitDFFromStringDF(dataFrame: DataFrame, DELIMITER:String): DataFrame ={
    val splitDF: DataFrame = dataFrame.select(split(col("value"), DELIMITER)
      .as("value"))
    splitDF
  }
  def getFinalDF(dataFrame: DataFrame, fieldDataArray: ArrayBuffer[Field], columnType: String): DataFrame = {
    val finalDF = dataFrame.select(fieldDataArray.map(i => col("value")
      .getItem(if (columnType == "array") i.index else i.fieldName)
      .cast(i.fieldType)
      .as(i.fieldName)): _*)
    finalDF
  }

  def getDFFromKafka(kafkaURL: String, kafkaTopic: String): DataFrame ={
    val df: DataFrame = spark.read.format("kafka")
      .option("kafka.bootstrap.servers",kafkaURL)
      .option("subscribe",kafkaTopic).load()
    val stringDF = df.select(col("value").cast("string"))
    stringDF
  }

  def getMappedDFFromStringDF(dataFrame: DataFrame): DataFrame={
    val mappedDF: DataFrame = dataFrame.withColumn("value",
      from_json(col("value"),
        MapType(StringType, StringType)))
      .select(col("value"))
    mappedDF
  }




}
