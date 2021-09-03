package com.mycompany.parse

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import scala.collection.mutable.Map

trait Mapper {
  val outputDir:String
  val inputFile:String
  val mapper:Map[String,String]
  val colNames: Array[String] = mapper.keySet.toArray
  var df: DataFrame
  def writeToParquet(): Unit = {
    df.write.parquet(outputDir)
    println("Parquet output written")
  }
}

private case class CSV(inputFile:String,outputDir:String,mapper:Map[String,String]) extends Mapper{
  override var df:DataFrame = spark.read.option("inferSchema",true).csv(inputFile+"/*").select(colNames.map(m=>col(m).as(mapper.getOrElse(m,m))):_*)
}

private case class TSV(inputFile:String,outputDir:String,mapper:Map[String,String]) extends Mapper{
  override var df: DataFrame = spark.read.option("sep", "\t").option("inferSchema",true).csv(inputFile+"/*")select(colNames.map(m=>col(m).as(mapper.getOrElse(m,m))):_*)
}

private case class JSON(inputFile:String,outputDir:String,mapper:Map[String,String]) extends Mapper{
  override val colNames:Array[String] = mapper.values.toArray
  override var df: DataFrame = spark.read.option("inferSchema",true).json(inputFile+"/*").select(colNames.map(m=>col(m)):_*)
}

object ParquetWriter{
  def apply(fileType:String,inputFile:String, outputDir:String,mapper:Map[String,String]): Mapper = fileType match{
    case "csv" => CSV(inputFile,outputDir,mapper)
    case "tsv" => TSV(inputFile,outputDir,mapper)
    case "json" => JSON(inputFile,outputDir,mapper)
  }
}