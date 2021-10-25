package com.mycompany.utils

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.mycompany.configuration.Field
import org.apache.spark.sql.types.{ArrayType, DoubleType, MapType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.FlatSpec

import scala.collection.mutable.{ArrayBuffer, WrappedArray}
import spark.implicits._
import ujson.Value.Value

class UtilsTest extends FlatSpec with DataFrameSuiteBase{
  def fixtures =
    new {
      val jsonString: String = scala.io.Source.fromResource("conf.json").mkString
      val configurationData: Value = ujson.read(jsonString)
      val fieldData: ArrayBuffer[Field] = configurationData("fieldData").arr.map(i => Field(i("name").str, i("type").str, i("index").str.toInt))

      val expectedSeq = Seq(Row(WrappedArray.make(Array[String]("1.33190100004E9","C1D7mK1PlzKEnEyG03","192.168.202.79","50471","192.168.229.251","80","1","HEAD","/DEASLog05.nsf","0","0"))),
        Row(WrappedArray.make(Array[String]("1.33190100009E9","CzhIEIizmxUoN6gP7","192.168.202.79","50479","192.168.229.251","80","1","HEAD","/DEESAdmin.nsf","0","0"))),
        Row(WrappedArray.make(Array[String]("1.33190100015E9", "CIuYWb1zOaIDFu8Dg5", "192.168.202.79", "50487", "192.168.229.251", "80", "1", "HEAD", "/domcfg.nsf", "0", "0"))))
      val schema = List(StructField("value",ArrayType(StringType),nullable = true))
      val expectedSplitDF: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(expectedSeq), StructType(schema))

      val mapSchema = List(StructField("value",MapType(StringType,StringType,valueContainsNull = true),nullable = true))
      val mapSeq = Seq(Row(Map("mac" -> "00:26:9e:83:a2:30", "ts" -> "1.33190104723E9", "id_resp_p" -> "67", "id_orig_p" -> "68", "assigned_ip" -> "192.168.202.76", "id_resp_h" -> "192.168.202.1", "id_orig_h" -> "192.168.202.76", "uid" -> "CCHNFI4C6RAO93bP7", "lease_time" -> "0.0", "trans_id" -> "276787")),
        Row(Map("mac" -> "00:26:b9:da:95:2c", "ts" -> "1.33190111774E9", "id_resp_p" -> "67", "id_orig_p" -> "68", "assigned_ip" -> "192.168.204.69", "id_resp_h" -> "192.168.204.1", "id_orig_h" -> "192.168.204.69", "uid" -> "CouYOF1J4EnQkQNSl3", "lease_time" -> "0.0", "trans_id" -> "202330")),
        Row(Map("mac" -> "f0:de:f1:2e:6a:5a", "ts" -> "1.33190112062E9", "id_resp_p" -> "67", "id_orig_p" -> "68", "assigned_ip" -> "192.168.202.102", "id_resp_h" -> "192.168.202.1", "id_orig_h" -> "192.168.202.102", "uid" -> "C9svD93TrEvPshF7Gf", "lease_time" -> "0.0", "trans_id" -> "7111068")))
      val expectedMapDF: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(mapSeq), StructType(mapSchema))
    }



  "getSplitDFFromStringDF" should "split a dataframe string csv column to individual columns" in {
    val f = fixtures
    val inputDF: DataFrame = Seq("1.33190100004E9,C1D7mK1PlzKEnEyG03,192.168.202.79,50471,192.168.229.251,80,1,HEAD,/DEASLog05.nsf,0,0",
      "1.33190100009E9,CzhIEIizmxUoN6gP7,192.168.202.79,50479,192.168.229.251,80,1,HEAD,/DEESAdmin.nsf,0,0",
      "1.33190100015E9,CIuYWb1zOaIDFu8Dg5,192.168.202.79,50487,192.168.229.251,80,1,HEAD,/domcfg.nsf,0,0").toDF("value")

    val outputDF = Utils.getSplitDFFromStringDF(inputDF, DELIMITER = ",")
    assertDataFrameEquals(outputDF, f.expectedSplitDF)
  }

  it should "split a dataframe string tsv column to individual columns" in {
    val f = fixtures
    val inputDF: DataFrame = Seq("1.33190100004E9    C1D7mK1PlzKEnEyG03    192.168.202.79    50471    192.168.229.251    80    1    HEAD    /DEASLog05.nsf    0    0",
                                  "1.33190100009E9    CzhIEIizmxUoN6gP7    192.168.202.79    50479    192.168.229.251    80    1    HEAD    /DEESAdmin.nsf    0    0",
                                  "1.33190100015E9    CIuYWb1zOaIDFu8Dg5    192.168.202.79    50487    192.168.229.251    80    1    HEAD    /domcfg.nsf    0    0").toDF("value")

    val outputDF = Utils.getSplitDFFromStringDF(inputDF, DELIMITER = "    ")
    assertDataFrameEquals(outputDF, f.expectedSplitDF)

  }

  "getMappedDFFromStringDF" should "create mapped dataframe from string dataframe" in {
    val f = fixtures
    val inputDF = Seq("""{"ts":1.33190104723E9,"uid":"CCHNFI4C6RAO93bP7","id_orig_h":"192.168.202.76","id_orig_p":68,"id_resp_h":"192.168.202.1","id_resp_p":67,"mac":"00:26:9e:83:a2:30","assigned_ip":"192.168.202.76","lease_time":0.0,"trans_id":276787}""",
                      """{"ts":1.33190111774E9,"uid":"CouYOF1J4EnQkQNSl3","id_orig_h":"192.168.204.69","id_orig_p":68,"id_resp_h":"192.168.204.1","id_resp_p":67,"mac":"00:26:b9:da:95:2c","assigned_ip":"192.168.204.69","lease_time":0.0,"trans_id":202330}""",
      """{"ts":1.33190112062E9,"uid":"C9svD93TrEvPshF7Gf","id_orig_h":"192.168.202.102","id_orig_p":68,"id_resp_h":"192.168.202.1","id_resp_p":67,"mac":"f0:de:f1:2e:6a:5a","assigned_ip":"192.168.202.102","lease_time":0.0,"trans_id":7111068}""").toDF("value")
    val outputDF = Utils.getMappedDFFromStringDF(inputDF)
    assertDataFrameEquals(outputDF,f.expectedMapDF)
  }

  "getFinalDF" should "give output df from array df" in {
    val f = fixtures
    val seq = Seq(Row(1.33190100004E9,"C1D7mK1PlzKEnEyG03"), Row(1.33190100009E9,"CzhIEIizmxUoN6gP7"), Row(1.33190100015E9, "CIuYWb1zOaIDFu8Dg5"))
    val schema = List(StructField("ts",DoubleType,true),StructField("uid",StringType,true))
    val expectedFinalDF = spark.createDataFrame(spark.sparkContext.parallelize(seq), StructType(schema))
    val outputDF = Utils.getFinalDF(f.expectedSplitDF,f.fieldData,columnType = "array")
    assertDataFrameEquals(expectedFinalDF, outputDF)
  }

  it should "give output df from map df" in {
    val f = fixtures
    val seq = Seq(Row(1.33190104723E9,"CCHNFI4C6RAO93bP7"), Row(1.33190111774E9,"CouYOF1J4EnQkQNSl3"), Row(1.33190112062E9, "C9svD93TrEvPshF7Gf"))
    val schema = List(StructField("ts",DoubleType,true),StructField("uid",StringType,true))
    val expectedFinalDF = spark.createDataFrame(spark.sparkContext.parallelize(seq), StructType(schema))
    val outputDF = Utils.getFinalDF(f.expectedMapDF,f.fieldData,columnType = "map")
    assertDataFrameEquals(expectedFinalDF, outputDF)
  }



}
