package com.ez.spark.pairrdd.transform

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object PairAggregations {
  	def main (args: Array[String]) {
  	val conf = new SparkConf().setAppName("LineCount").setMaster("local")

				//create spark context object
				val sc = new SparkContext(conf)

				//	Create a Pair RDD from collection
				val data = Array(("key1", 1),("key1", 5),("key1", 2),("key2", 3),("key2", 1))
				val pairRdd = sc.parallelize(data)
   	
				//reduce by key ... implicit group by and applying reduce on each value withing group
				val reducedRdd = pairRdd.reduceByKey((accum, value) => accum + value)
				
				println(reducedRdd.collect().mkString(" "))  

 }
}