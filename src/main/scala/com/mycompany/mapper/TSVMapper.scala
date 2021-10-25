package com.mycompany.mapper

import com.mycompany.utils.Utils
import org.apache.spark.sql.DataFrame

class TSVMapper() extends Mapper{
  val splitDF: DataFrame = Utils.getSplitDFFromStringDF(inputDF,DELIMITER = "\t")
  override var finalDF: DataFrame = Utils.getFinalDF(splitDF, fieldData,columnType = "array")
}

