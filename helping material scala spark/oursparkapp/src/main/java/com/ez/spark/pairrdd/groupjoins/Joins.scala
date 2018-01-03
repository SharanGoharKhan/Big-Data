package com.ez.spark.pairrdd.groupjoins

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Joins {
  	def main (args: Array[String]) {
   val conf = new SparkConf().setAppName("LineCount").setMaster("local")

				//create spark context object
				val sc = new SparkContext(conf)

				//creating RDD by using external source
				val customers = sc.textFile("src/main/resources/customers.txt")
				val orders = sc.textFile("src/main/resources/orders.txt")
				
				val custPairRdd = customers.map(line=>(line.split("\t")(0),line.split("\t")(1)+" "+ line.split("\t")(2)))
				val orderPairRdd = orders.map(line=>(line.split("\t")(1),line.split("\t")(0)+" "+ line.split("\t")(2)))
				
				
				val joinedRdd = custPairRdd.join(orderPairRdd)
				println(joinedRdd.collect().mkString("\n"))  

 }
}