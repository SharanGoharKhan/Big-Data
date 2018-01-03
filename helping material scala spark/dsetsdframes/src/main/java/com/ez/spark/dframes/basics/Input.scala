package com.ez.spark.dframes.basics

import org.apache.spark.sql._
import com.databricks.spark.csv


object Input {
	def main (args: Array[String]) {
		val session = SparkSession.builder
				.master("local")
				.getOrCreate()

//				val jsonDataFrame = session.read
//				.json("src/main/resources/input/data.json")
//				
//				jsonDataFrame.show()





							//A CSV File
				//				val csvDataFrame = session.read.format("csv")
				//				.option("header", "true") //reading the headers
				//				.option("mode", "DROPMALFORMED")
				//				.load("src/main/resources/assignment/movies.csv")
				//
				//				//csvDataFrame.printSchema()
				//
				//				//SQL version
				//				val csvSQLDataFrame = session.sql("SELECT * FROM csv.`src/main/resources/assignment/movies.csv`")
				//				csvSQLDataFrame.printSchema()

				
				
				
				
				
						val customersInfo = session.read
								.format("com.databricks.spark.csv")
								.option("delimiter", "\t")
								.option("header", "true")
				        .load("src/main/resources/input/customers.txt")
	
				        customersInfo.show()
				        
				        
	}
}