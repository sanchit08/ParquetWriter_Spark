package com.mycompany.mapper
import com.mycompany.configuration.{ConfigurationParameters, Field}
import com.mycompany.utils.Utils
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ArrayBuffer

trait Mapper {
  val outputDir: String = ConfigurationParameters.outputDirectory
  val kafkaTopic: String = ConfigurationParameters.kafkaTopic
  var fieldData: ArrayBuffer[Field] = ConfigurationParameters.fieldData
  val kafkaURL:String = ConfigurationParameters.kafkaURL
  val inputDF: DataFrame = Utils.getDFFromKafka(kafkaURL, kafkaTopic)
  var finalDF: DataFrame

}

object ParquetWriter{

  def apply(fileType:String): Mapper = fileType match{
    case "csv" => new CSVMapper()
    case "tsv" => new TSVMapper()
    case "json" => new JSONMapper()
  }

}