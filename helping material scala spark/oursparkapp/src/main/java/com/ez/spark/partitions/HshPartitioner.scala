package com.ez.spark.partitions

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner

object HshPartitioner {
  	def main (args: Array[String]) {
   //Create conf object
		val conf = new SparkConf().setAppName("LineCount").setMaster("local")

				//create spark context object
				val sc = new SparkContext(conf)

				val xList = (0 to 1000).toList
				val sampleRdd = sc.parallelize(xList,10)
				val pairRdd = sampleRdd.map(value=>(value%10,value))
				
				val hashedRdd = pairRdd.partitionBy(new HashPartitioner(10))
				//returns Partitioner if any, HashPartitioner, RangePartitioner,..
				println(hashedRdd.partitioner)
				hashedRdd.saveAsTextFile("src/main/resources/partitoner")
 }
}