package com.ez.spark.dframes.operations

import org.apache.spark.sql._

object TempView {
	def main (args: Array[String]) {
		val sparkSession = SparkSession.builder.master("local").getOrCreate()
				sparkSession.sparkContext.setLogLevel("ERROR")
				import sparkSession.implicits._
				val jsonDataFrame = sparkSession.read.json("src/main/resources/input/data.json")

				jsonDataFrame.createOrReplaceTempView("people")
				val sqlDataFrame = sparkSession.sql("SELECT * FROM people")
				sqlDataFrame.show()


	}
}