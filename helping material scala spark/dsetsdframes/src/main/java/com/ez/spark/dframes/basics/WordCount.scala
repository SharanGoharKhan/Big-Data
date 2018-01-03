package com.ez.spark.dframes.basics
import com.databricks.spark.csv

import org.apache.spark.sql._

object WordCount {
	def main (args: Array[String]) {
		val sparkSession = SparkSession.builder.master("local").getOrCreate()

		val customersInfo = sparkSession.read
				.format("com.databricks.spark.csv")
	      .load("src/main/resources/input/input.txt")
				
        //customersInfo.createOrReplaceTempView("Input")
        //val sqlDFrame = sparkSession.sql("select * from Input")
        //val renamedDFrame = sqlDFrame.withColumnRenamed("_c0", "Lines")
        
	      val words = customersInfo.explode("_c0", "words")((line:String) => line.split(" "))
       //customersInfo.show()
		
            
        
      words.show()
	}
}