package com.ez.spark.partitions

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object RepartCol {
  	def main (args: Array[String]) {
  	  //Create conf object
		val conf = new SparkConf().setAppName("LineCount").setMaster("local")

				//create spark context object
				val sc = new SparkContext(conf)
  	  
   val xList = (1 to 1000).toList
   val sampleRdd = sc.parallelize(xList,10)
   
   //The coalesce method reduces the number of partitions
   val coalesceRdd = sampleRdd.coalesce(4)
   
   //either increase or decrease the number of partitions
   //val repartRdd = sampleRdd.coalesce(20)//wont work
   val repartRdd = sampleRdd.repartition(20)
   
   //sampleRdd.saveAsTextFile("src/main/resources/RepartCol")

   
   println(repartRdd.partitions.size) 
   
 }
}