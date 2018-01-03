package com.ez.spark.dframes.operations

import org.apache.spark.sql.SparkSession

object CommonOps {
	def main (args: Array[String]) {
		val sparkSession = SparkSession.builder.master("local").getOrCreate()
		sparkSession.sparkContext.setLogLevel("ERROR")

		import sparkSession.implicits._
		
				val jsonDataFrame = sparkSession.read.json("src/main/resources/input/data.json")
				//jsonDataFrame.printSchema()
				jsonDataFrame.show()
				jsonDataFrame.select("name").show()
				jsonDataFrame.select($"name", $"age" + 1).show()
				jsonDataFrame.filter($"age" > 21).show()
				jsonDataFrame.groupBy("age").count().show()
	}
}