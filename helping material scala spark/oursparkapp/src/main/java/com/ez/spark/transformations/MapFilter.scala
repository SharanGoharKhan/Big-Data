package com.ez.spark.transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object MapFilter {
  def main(args: Array[String]) {

		//Create conf object
		val conf = new SparkConf().setAppName("LineCount").setMaster("local")

				//create spark context object
				val sc = new SparkContext(conf)

				//creating RDD by using external source
				val lines = sc.textFile("src/main/resources/input.txt")
				val filteredLines = lines.filter(record => record.contains("Hello"))
				val mappedLines = filteredLines.map(record => record.toUpperCase())
				
				println(filteredLines.collect().mkString(","))
				
				
  }
}