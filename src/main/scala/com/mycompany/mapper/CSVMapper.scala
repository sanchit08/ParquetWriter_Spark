package com.mycompany.mapper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

case class CSVMapper() extends Mapper{
  override var df:DataFrame = spark.read.option("inferSchema",value = true)
    .csv(inputFile+"/*")
    .select(colNames.map(m=>col(m).as(columnNames.getOrElse(m,m))):_*)
}
