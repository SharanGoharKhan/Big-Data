package com.ez.spark.dframes.operations

import org.apache.spark.sql._

object Selection {
	def main (args: Array[String]) {
		val sparkSession = SparkSession.builder.master("local").getOrCreate()

				sparkSession.sparkContext.setLogLevel("ERROR")
				import sparkSession.implicits._
				val jsonDataFrame = sparkSession.read.json("src/main/resources/input/data.json")

				jsonDataFrame.createOrReplaceTempView("people")
				
				val sqlDataFrame = sparkSession
				.sql("SELECT * FROM people")
				//.sql("SELECT name AS PersonName FROM people")
				//.sql("SELECT count(name) FROM people where name = 'Andy'")
				//.sql("SELECT name FROM people where name LIKE '%nd%'")
				//.sql("SELECT name FROM people WHERE name IN ('Andy', 'Michael')")
				//.sql("SELECT name FROM people WHERE age BETWEEN 15 AND 30")
				//.sql("SELECT DISTINCT name FROM people")
				//.sql("SELECT DISTINCT * FROM people ORDER BY age DESC")
				//.sql("SELECT * FROM people LIMIT 2")
				//.sql("SELECT MIN(age) FROM people")
				//.sql("SELECT SUM(age) FROM people")
				//.sql("SELECT AVG(age) FROM people")
				//.sql("SELECT Count(name) FROM people GROUP BY name")
				//.sql("SELECT Count(name) FROM people GROUP BY name HAVING Count(name)>1")

				sqlDataFrame.show()


	}
}