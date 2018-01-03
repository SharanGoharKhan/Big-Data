package com.ez.spark.pairrdd.transform

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object PairRDDTransformations {
	def main (args: Array[String]) {
		val conf = new SparkConf().setAppName("LineCount").setMaster("local")

				//create spark context object
				val sc = new SparkContext(conf)

				//	Create a Pair RDD from collection
				val data = Array(("key1", "value1"),("key2", "value2"),("key3", "value3"))
				val pairRdd = sc.parallelize(data)

				//Filter and Map, we are using tuple._1 but can use case(key, value)
				val ftdPairRdd = pairRdd.filter(tuple => tuple._1=="key1")
				val mpdPairRdd = pairRdd.map(tuple => (tuple._2, tuple._1))

				//To get values from mapping on a tuple, same can be done for keys
				val valuesRdd = pairRdd.map(tuple => tuple._2)
				
				//using values and keys builtin functions
				val valuedOfRdd = pairRdd.values
				val keysOfRdd = pairRdd.keys
				
				//applying a function only on values 
				val updatedRdd = pairRdd.mapValues(value=>value.toUpperCase())
				
				//sort by key desc
				val sortedRDD = pairRdd.sortByKey(false)
				
				//flatmap on a pair
				val flapMappedRdd = pairRdd.flatMapValues(value => value.split("lu"))

				println(flapMappedRdd.collect().mkString(" "))  

	}
}