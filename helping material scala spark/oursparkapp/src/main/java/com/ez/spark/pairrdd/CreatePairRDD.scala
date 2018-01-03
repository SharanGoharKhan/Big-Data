
package com.ez.spark.pairrdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CreatePairRDD {
	def main (args: Array[String]) {

		//Create conf object
		val conf = new SparkConf().setAppName("LineCount").setMaster("local")

				//create spark context object
				val sc = new SparkContext(conf)

				//creating RDD by using external source
				val lines = sc.textFile("src/main/resources/input.txt")

				val pairRDD = lines.map(line=>{
					(line.split(" ")(0),line.split(" ")(1),line.split(" ")(2)) 
				})
				//val data = Array(("key1", "value1"),("key2", "value2"),("key3", "value3"))
				//val pairRDDfromdata = sc.parallelize(data)
				
				println(pairRDD.collect().mkString(" "))  


	}
}