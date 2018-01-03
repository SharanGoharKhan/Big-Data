
package com.ez.spark.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object PrintRecords {
	def main(args: Array[String]) {

		//Create conf object
		val conf = new SparkConf().setAppName("LineCount").setMaster("local")

				//create spark context object
				val sc = new SparkContext(conf)

				//creating RDD by using a collection
				val myArray = Array(1,2,3,4,5)
				val myRDD = sc.parallelize(myArray)
				
				
				//println(myRDD.first())
				for (someVal <- myRDD.collect())
				  //println(someVal)
				//myRDD.foreach(someRecord=>println(someRecord))
				  
				println(myRDD.collect().mkString(","))  
	}
}