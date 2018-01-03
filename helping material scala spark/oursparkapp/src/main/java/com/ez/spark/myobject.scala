
package com.ez.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object myobject {
  def filterFunc(input:String) = input.contains("Hello")
  def main(args: Array[String]) {
   
        //Create conf object
	      	val conf = new SparkConf().setAppName("LineCount").setMaster("local")

				//create spark context object
				val sc = new SparkContext(conf)
				
	      	//creating RDD by using external source
				val lines = sc.textFile("src/main/resources/input.txt")
				
				//creating RDD by using a collection
				var myArray = Array(1,2,3,4,5)
				val myRDD = sc.parallelize(myArray)
				val sum = myRDD.reduce ((x, y) => x + y)
				
				myRDD.countByValue().foreach(println)
				
				//mapping transformation
				val doubleRDD = myRDD.map(f=>f+f)
				
				//printing rdd - another approach
				//println(doubleRDD.collect().mkString(","))
				
				//filter and union transformations on a RDD
//				val hellolines = lines.filter(f => f.contains("Hello"))
//				val Scalalines = lines.filter(f => f.contains("Scala"))
//				val allLines = hellolines.union(Scalalines)
//				
				//some actions on the RDDs
				//println("LinesCount:"+allLines.count())
				//println("FirstLine:"+allLines.first())

				
				
				//for (i <- lines.collect()){
				//  println(i)
				//}
				//allLines.saveAsTextFile("src/main/resources/output.txt")
				
				//val filtlines = lines.filter(filterFunc)
				//allLines.foreach(println)
      
    sc.stop()
  }
 
}