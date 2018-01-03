
package com.ez.spark.pairrdd.transform

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object wordcount {
  def main (args: Array[String]) {
    
        //Create conf object
	      	val conf = new SparkConf().setAppName("LineCount").setMaster("local")
				//create spark context object
				val sc = new SparkContext(conf)
	  
	 		val lines = sc.textFile("src/main/resources/input.txt")
	 		val words = lines.flatMap(line => line.split(" "))
	 		val wordsPair = words.map(word => (word,1))
	 		val wordcount = wordsPair.reduceByKey((accum,count) => accum+count)
	 		println(wordcount.collect().mkString("\n"))
	      	
	      	
	      	
  }
}