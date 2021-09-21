# ParquetWriter

This repo contains code for a Spark job that converts an input CSV, TSV or JSON file into [Parquet](https://parquet.apache.org/) format. The spark job is configuration driven using a JSON file.

## Packages Required

In addition to Spark 3.1.1 and Scala 2.1.12 the additional packages required are :

* [sbt](https://www.scala-sbt.org/) : An interactive build tool for Scala and Java projects.
* [scaltest](https://www.scalatest.org/) : A testing tool to be used in Scala ecosystem.

## Configuration File
A JSON file is used as a configuration file for providing necessary arguements for running the Spark job. The following are the necessary fields in the configuration file.

* fileType : Should contain the input file format type. This can be either csv, tsv or json
* inputDir : Relative path to the directory where the input file resides.
* outputDir : Relative path to the directory where the output parquet files must be stored
* fieldNames : A JSON array of Field objects that contain information regarding the fields to be extracted from the input log file. Each Field object consists of three fields
	* name : Column name of the column to be extracted
	* type : Type of the data in the column
	* index : The index of the column starting from 0

Here is an example of how a configuration file must look

```
{		
		"fileType" : "tsv",
		"inputDir" : "input",
		"outputDir" : "output",
		"fieldNames" : [
			{	
				"name" : "ts",
				"type" : "double",
				"index" : "0"
			},
			{	
				"name" : "uid",
				"type" : "string",
				"index" : "1"
			},
			{
				"name" : "id_resp_p",
				"type" : "long",
				"index" : "5"
			}
		]
}
```
## Usage

### Compiling the program

The project can be compiled and packaged using the sbt build tool. Run the following command from the root of the project directory

```
sbt package
```
The packaged jar file will be present in the target folder.

### Submitting the spark job

The job can be run using spark-submit from the command line. The syntax for submitting a spark job is

```
$SPARK_HOME/bin/spark-submit \
  --master <master-url> \
  parquetwriter_spark.jar \
  [relative-path-to-configuration-file]
```

