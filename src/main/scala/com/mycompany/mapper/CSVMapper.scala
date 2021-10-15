package com.mycompany.mapper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, split}

case class CSVMapper() extends Mapper{
  override var finalDF: DataFrame = df.select(split(col("value"),",")
    .as("value"))
    .select(fieldData.map(i => col("value")
      .getItem(i.index)
      .cast(i.fieldType)
      .as(i.fieldName)):_*)
}
