package com.ez.spark.dframes.create

import org.apache.spark.sql._

object CreateDF {
	def main (args: Array[String]) {
		val sparkSession = SparkSession.builder.master("local").getOrCreate()
				sparkSession.sparkContext.setLogLevel("ERROR")

				//From a textfile, same cane be done for other formats
				val customersInfo = sparkSession.read
				.format("com.databricks.spark.csv")
				.option("delimiter", "\t")
				.option("header", "true")
				.load("src/main/resources/input/customers.txt")
				
				//From existing data - you need to import SparkSession implicits
				import sparkSession.implicits._
				
				val existingData = List(1,2,3,4,5)
				val collectionDF = existingData.toDF("Values")
				val someTuples = List(("ez",1), ("bz",2))
				val tuplesCollectionDF = someTuples.toDF("name","id")
				
				//from RDD 
				val sampleRDD = sparkSession.sparkContext.parallelize(someTuples)
				val rddDataFrame = sampleRDD.toDF("studentName", "CNIC")
				rddDataFrame.show()
	}
}