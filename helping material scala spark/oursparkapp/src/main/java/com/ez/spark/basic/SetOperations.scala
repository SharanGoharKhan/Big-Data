
package com.ez.spark.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SetOperations {
	def main (args: Array[String]) {

		val conf = new SparkConf().setAppName("SetOps").setMaster("local")
				val sc = new SparkContext(conf)

				val set1 = Array("coffee", "coffee", "panda", "monkey", "tea")
				val set2 = Array("coffee", "monkey", "kitty")
				val rdd1 = sc.parallelize(set1)
				val rdd2 = sc.parallelize(set2)

				//println(rdd1.distinct().collect().mkString(","))
				//println(rdd1.union(rdd2).collect().mkString(","))
				//println(rdd1.intersection(rdd2).collect().mkString(","))
				//println(rdd1.subtract(rdd2).collect().mkString(","))
	}
}