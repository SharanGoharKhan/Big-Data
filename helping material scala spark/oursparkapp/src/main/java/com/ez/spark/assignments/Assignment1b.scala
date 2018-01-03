package com.ez.spark.assignments

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Assignment1b {
	def main (args: Array[String]) {

		//Create conf object
		val conf = new SparkConf().setAppName("LineCount").setMaster("local")

				//create spark context object
				val sc = new SparkContext(conf)

				//creating RDD by using external source
				val inputData = sc.textFile("src/main/resources/movies.csv")
				val directorImdb = inputData.map(line=>(line.split(",")(1),line.split(",")(24).toDouble))
				val groupedData = directorImdb.groupByKey()
				val result = groupedData.map(tuple => {
				  val valuesBuffer = tuple._2
  				  val average = valuesBuffer.sum/valuesBuffer.size
	 				(average,tuple._1)			  
				})
				
				println(result.sortByKey(false).collect().mkString("\n"))  


	}
}