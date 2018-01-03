package com.ez.spark.dframes.assignments

import org.apache.spark.sql._

object Assignment3 {
  	def main (args: Array[String]) {
  	  	 val sparkSession = SparkSession.builder.master("local").getOrCreate()
  	
				val csvDataFrame = sparkSession.read.format("csv")
				.option("header", "true") //reading the headers
				.option("mode", "DROPMALFORMED")
				.load("src/main/resources/assignment/abcnews.csv")
				
				

 }
}