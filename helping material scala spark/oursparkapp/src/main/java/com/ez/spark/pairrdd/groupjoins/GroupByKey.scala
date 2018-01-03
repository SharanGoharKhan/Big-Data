package com.ez.spark.pairrdd.groupjoins

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions


object GroupByKey {
  	def main (args: Array[String]) {
   	val conf = new SparkConf().setAppName("LineCount").setMaster("local")

				//create spark context object
				val sc = new SparkContext(conf)

				//	Create a Pair RDD from collection
				val data = Array(("key1", 1),("key1", 5),("key1", 2),("key2", 3),("key2", 1))
				val pairRdd = sc.parallelize(data)
   	
				val groupedRdd = pairRdd.groupByKey()
				//val countedRdd = groupedRdd.map(tuple => (tuple._1,tuple._2.size))
				
				println(groupedRdd.collect().mkString(" "))  
 }
}