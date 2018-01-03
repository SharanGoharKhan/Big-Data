package com.ez.spark.partitions

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Properties {
	def main (args: Array[String]) {
		//Create conf object
		val conf = new SparkConf().setAppName("LineCount").setMaster("local")

				//create spark context object
				val sc = new SparkContext(conf)

				val xList = (1 to 1000).toList
				val sampleRdd = sc.parallelize(xList,10)

				//sampleRdd.partitions returns an array with all partition references of source RDD
				println(sampleRdd.partitions.size)
				//returns Partitioner if any, HashPartitioner, RangePartitioner,..
				println(sampleRdd.partitioner)
				//returns default level of parallelism defined on SparkContext.
				//By default it is number of cores available to application.
				println(sc.defaultParallelism)

	}
}