package com.mycompany

import org.apache.spark.sql.SparkSession

package object configuration {
  val spark: SparkSession = SparkSession.builder().appName("Parser").getOrCreate()
  //spark.sparkContext.setLogLevel("ERROR")
}
