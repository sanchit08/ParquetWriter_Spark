package com.mycompany.mapper

import com.mycompany.utils.Utils
import org.apache.spark.sql.DataFrame

class JSONMapper() extends Mapper{
  val mappedDF: DataFrame = Utils.getMappedDFFromStringDF(inputDF)
  override var finalDF: DataFrame = Utils.getFinalDF(mappedDF,fieldData,columnType = "map")
}