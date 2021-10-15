package com.mycompany.mapper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{MapType, StringType}

case class JSONMapper() extends Mapper{
  val mappedDF: DataFrame = df.withColumn("value",
    from_json(col("value").cast("string"),
      MapType(StringType, StringType)))
    .select(col("value"))

  override var finalDF: DataFrame = mappedDF.select(fieldData.map(i => col("value")
    .getItem(i.fieldName)
    .cast(i.fieldType)
    .as(i.fieldName)): _*)

}