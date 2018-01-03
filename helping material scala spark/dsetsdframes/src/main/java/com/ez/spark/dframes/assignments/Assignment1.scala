package com.ez.spark.dframes.assignments

import org.apache.spark.sql._

object Assignment1 {
	def main (args: Array[String]) {
		val sparkSession = SparkSession.builder.master("local").getOrCreate()

				val csvDataFrame = sparkSession.read
				.format("com.databricks.spark.csv")
				.option("header", "true") //reading the headers
				.option("mode", "DROPMALFORMED")
				.load("src/main/resources/assignment/movies.csv")

				//val fltrdDataframe = csvDataFrame.filter(csvDataFrame("duration")>100)
				//println(fltrdDataframe.count())
				
				csvDataFrame.createOrReplaceTempView("movies")
				val sqlDataFrame = sparkSession.sql("select COUNT(duration) from movies where duration > 100")
				
				sqlDataFrame.show()
	}
}