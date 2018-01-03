package com.ez.spark.pairrdd.groupjoins

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object CoGroup {
	def main (args: Array[String]) {
		val conf = new SparkConf().setAppName("LineCount").setMaster("local")

				//create spark context object
				val sc = new SparkContext(conf)

				//	Create a Pair RDD from collection
				val pairRdd1 = sc.parallelize( Array(("key1", 1),("key1", 10),("key2", 1),("key3", 1)))
				val pairRdd2 = sc.parallelize( Array(("key3", 2),("key1", 2)))

				val cogroupedRdd = pairRdd1.cogroup(pairRdd2)
				val countedRdd = cogroupedRdd.map(keybufferstuple=>{ // keybufferstuple is a tuple

					val key = keybufferstuple._1 //string - key1,key2..
					val valsfrmGroups = keybufferstuple._2 //tuple of compactbuffers
					val firstGroupVals = valsfrmGroups._1 //values in the tuple from first Rdd, a compactbuf
					val secondGroupVals = valsfrmGroups._2//values from second Rdd, a compactbuf
					val count = 	firstGroupVals.size + 	secondGroupVals.size
					(key,count)
				})
				println(countedRdd.collect().mkString(" "))  

	}
}