
package com.ez.spark.actions

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Reduce {
	def main (args: Array[String]) {
		//Create conf object
		val conf = new SparkConf().setAppName("LineCount").setMaster("local")

				//create spark context object
				val sc = new SparkContext(conf)

				//creating RDD by using a collection
				val myArray = Array(1,2,3,4,5)
				val myRDD = sc.parallelize(myArray)

				//println(myRDD.reduce((value1, value2)=>value1+value2))
				val max = myRDD.reduce((accum, value) => { 
				  if (value > accum) value else accum
				})
				println("Max:" + max)
	}
}