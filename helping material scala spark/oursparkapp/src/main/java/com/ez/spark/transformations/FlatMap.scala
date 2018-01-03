
package com.ez.spark.transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object FlatMap {

	def applyonMap(param:String) :String = {
			return param.toUpperCase()
	}

	def main (args: Array[String]) {

		//Create conf object
		val conf = new SparkConf().setAppName("LineCount").setMaster("local")
				//create spark context object
				val sc = new SparkContext(conf)

				val lines = sc.textFile("src/main/resources/input.txt")
				val words = lines.flatMap(line=>line.split(" "))
				println(words.countByValue().mkString(","))

	}
}





