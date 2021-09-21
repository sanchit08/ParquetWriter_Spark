package com.mycompany.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory
import scala.collection.mutable.Map

object Utils {
  private val logger = LoggerFactory.getLogger(Utils.getClass)
  def getColumns(rdd: RDD[Row]):Map[String,String]={
    val map = Map[String,String]()
    for (row <- rdd.collect()) {
      val name = row.getString(0)
      val index = "_c" + row.getString(2)
      map += (index -> name)
    }
    logger.info("Successfully read all fields from configuration file")
    map
  }
}
