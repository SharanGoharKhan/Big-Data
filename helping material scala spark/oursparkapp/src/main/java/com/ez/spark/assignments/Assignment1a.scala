package com.ez.spark.assignments

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.StringReader

object Assignment1a {
	def main (args: Array[String]) {

		//Create conf object
		val conf = new SparkConf().setAppName("LineCount").setMaster("local")

				//create spark context object
				val sc = new SparkContext(conf)

				//creating RDD by using external source
				val inputData = sc.textFile("src/main/resources/movies.csv")
				val moviesDuration = inputData.map(line=>{
				  (line.split(",")(11),line.split(",")(3) )
				  })
				
				val filteredMovies = moviesDuration.filter(record=>{
				   if (record._2=="") false else record._2.toInt>=300
				})
		    println(filteredMovies.collect().mkString("\n"))  

	}
}