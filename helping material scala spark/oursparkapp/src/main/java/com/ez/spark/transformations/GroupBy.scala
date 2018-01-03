package com.ez.spark.transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object GroupBy {
	def main (args: Array[String]) {

		//Create conf object
		val conf = new SparkConf().setAppName("LineCount").setMaster("local")
				//create spark context object
				val sc = new SparkContext(conf)


				//	Create a Pair RDD from collection
				val data = Array("Alpha","Bravo","Charlie","Apple","Banana")
				val pairRdd = sc.parallelize(data)
				val groupedRdd = pairRdd.groupBy(value => value.charAt(0))
		
				println(groupedRdd.collect().mkString(" ")) 
				

	}
}