package com.ez.spark.dframes
import org.apache.spark.sql._

object Session {
  	def main (args: Array[String]) {
   
  	 val sparkSession = SparkSession.builder.master("local").getOrCreate()
      println(sparkSession)
 }
}