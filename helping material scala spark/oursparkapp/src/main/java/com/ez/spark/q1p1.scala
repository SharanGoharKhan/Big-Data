package com.ez.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object q1p1 {
	def main (args: Array[String]) {

		val conf = new SparkConf().setAppName("duration_count").setMaster("local")
				val sc = new SparkContext(conf)

				val lines = sc.textFile("src/main/resources/movies.csv")
				val parse = lines.map(line =>line.split(",")(3))
				//val result = parse.filter(p => p.toDouble > 100)
				
				val result = parse.filter(f => {
				     if (f.equals(""))
				       false
				     else
				       f.toInt > 100
				     } )
				  
				  
				result.count()
				result.collect().foreach(println)


				//   val rcrds = sc.textFile("src/main/resources/movies.csv")
				//   val durations = rcrds.map(f =>f.split(',')(3))
				//   val count = durations.filter(f => {
				//     if (f.equals(""))
				//       false
				//     else
				//       f.toInt > 100
				//     } ).count()
				//   println(count)
	}
}