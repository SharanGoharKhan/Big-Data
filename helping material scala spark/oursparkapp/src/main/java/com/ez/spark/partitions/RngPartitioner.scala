package com.ez.spark.partitions

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner
import org.apache.spark.RangePartitioner


object RngPartitioner {
  	def main (args: Array[String]) {
    //Create conf object
		val conf = new SparkConf().setAppName("LineCount").setMaster("local")

				//create spark context object
				val sc = new SparkContext(conf)

				val xList = Array((8,"A"), (96,"B"), (240,"C"), (401, "D"), (800, "E"))
				val pairRdd = sc.parallelize(xList,10)
				
				//val rangedRdd = pairRdd.partitionBy(new HashPartitioner(4))
				val rangedRdd = pairRdd.partitionBy(new RangePartitioner(8,pairRdd))
				rangedRdd.saveAsTextFile("src/main/resources/rngPartitoner")
 }
}