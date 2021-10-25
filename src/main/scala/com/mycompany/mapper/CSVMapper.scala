package com.mycompany.mapper

import com.mycompany.utils.Utils
import org.apache.spark.sql.DataFrame

class CSVMapper() extends Mapper{
    val splitDF: DataFrame = Utils.getSplitDFFromStringDF(inputDF,DELIMITER = ",")
    override var finalDF: DataFrame = Utils.getFinalDF(splitDF, fieldData,columnType = "array")

}
