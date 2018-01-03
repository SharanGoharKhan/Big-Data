package com.ez.spark.dsets

import org.apache.spark.sql._
import org.apache.spark.sql.types._

case class Person(name: String, age: Long)

object CreateDataSet {
  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.master("local").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    //create a DataSet from a DataFrame
    import sparkSession.implicits._
    val jsonDataSet = sparkSession
      .read.json("src/main/resources/input/data.json")
      .as[Person]

   

    //val filteredRecords = jsonDataSet.filter(jsonDataSet("name") === "Andy")
    //filteredRecords.show()

    //val filteredRecords = jsonDataSet.filter(record => record.name.equals("Andy"))
    //filteredRecords.show()
  }
}