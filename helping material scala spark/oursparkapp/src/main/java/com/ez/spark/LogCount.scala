package com.ez.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object LogCount {
	def main (args: Array[String]) {
		//Create conf object
		val conf = new SparkConf().setAppName("LineCount").setMaster("local")

				//create spark context object
				val sc = new SparkContext(conf)

				val input = sc.textFile("src/main/resources/logfile.txt")
				val logWords = input.map(line => line.split(" ")(2))
				val pairedLogWords = logWords.map(words => (words, 1))
				val resultRdd = pairedLogWords.reduceByKey{(accum,value) => accum + value}
				println(resultRdd.collect().mkString(" "))  

				println(resultRdd.toDebugString)  

	}
}