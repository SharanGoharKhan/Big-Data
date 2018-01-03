package com.ez.spark.pairrdd.actions

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object PairRddActions {
  	def main (args: Array[String]) {
   
  	  //Create conf object
		val conf = new SparkConf().setAppName("LineCount").setMaster("local")

				//create spark context object
				val sc = new SparkContext(conf)

				val data = Array(("key1", "value1"),("key1", "value1"),("key2", "value2"),("key3", "value3"))
				val pairRDDfromdata = sc.parallelize(data)
				println(pairRDDfromdata.count())
				println(pairRDDfromdata.countByKey())
				println(pairRDDfromdata.lookup("key1").mkString)
				
 }
}