package com.mycompany
import org.apache.spark.sql.SparkSession
package object parse {
  val spark: SparkSession = SparkSession.builder().appName("Parser").getOrCreate()
}
