package com.mycompany.mapper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

case class JSONMapper() extends Mapper{
  override val colNames:Array[String] = columnNames.values.toArray
  override var df: DataFrame = spark.read.option("inferSchema",value = true)
    .json(inputFile+"/*")
    .select(colNames.map(m=>col(m)):_*)

}