package com.ez.spark.assignments

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Assignment1c {
	def main (args: Array[String]) {
		//Create conf object
		val conf = new SparkConf().setAppName("LineCount").setMaster("local")

				//create spark context object
				val sc = new SparkContext(conf)

				//creating RDD by using external source
				val originalData = sc.textFile("src/main/resources/abcnews.csv")
				val newsData = originalData.filter(line=> line.split(",").size==2)

				val positives = sc.textFile("src/main/resources/positives.txt").collect().mkString(":")
				val negatives = sc.textFile("src/main/resources/negatives.txt").collect().mkString(":")

				newsData.persist()
				val news = newsData.map(line=>(line.split(",")(1),line.split(",")(1)))
				val newsWords = news.flatMapValues(headline => headline.split(" "))

				val newsWordSentiment = newsWords.map(hwordTuple=>{
					val word = hwordTuple._2	
					
							var wordSentiment = 0
							if (positives.contains(":"+word+":"))  
								wordSentiment=1 
								else if (negatives.contains(":"+word+":"))  
									wordSentiment = -1

									(hwordTuple._1,wordSentiment)
				})
				
				val groupedSentiments = newsWordSentiment.groupByKey()
				val result = newsWordSentiment.reduceByKey((accum,count)=>accum+count)
			
				println(result.collect().mkString("\n"))  


	}
}