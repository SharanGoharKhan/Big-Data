package com.ez.spark.dframes.create

import org.apache.spark.sql._
import org.apache.spark.sql.types._

object Schema {
	def main (args: Array[String]) {
		val sparkSession = SparkSession.builder.master("local").getOrCreate()

				//val jsonDataFrame = sparkSession.read.json("src/main/resources/input/data.json")
				//	jsonDataFrame.printSchema()

				//what if the schema inferred needs improvement
				val personName = StructField("name", DataTypes.StringType)
				val personAge = StructField("age", DataTypes.IntegerType)
				val personSchema = StructType(Array(personName,personAge))

				val jsonDataFrame = sparkSession
				.read.schema(personSchema)
				.json("src/main/resources/input/data.json")
				
				jsonDataFrame.createOrReplaceTempView("Persons")


				//if we need to rename a column name
				val renamedDataFrame = jsonDataFrame.withColumnRenamed("name", "PersonName")
				val renamedSQLFrame = sparkSession.sql("SELECT name AS SQLName, age FROM Persons")
				
			  //if we need to add a new column probably based on previous ones
				val updatedDataFrame = jsonDataFrame.withColumn("NewColumn", jsonDataFrame("name"))
				val updatedSQLFrame =   sparkSession.sql("SELECT name AS SQLName, name, age FROM Persons")

				//if we need to update type info for a column name
				val typedSQLFrame =   sparkSession.sql("SELECT name AS SQLName, CAST (age AS LONG) FROM Persons")

				
				typedSQLFrame.printSchema()


				//				jsonDataFrame.createOrReplaceTempView("Persons")
				//				val sqlDataFrame = sparkSession.sql("Select PrsonId from Persons")
				//				sqlDataFrame.show()

				//				//import sparkSession.implicits._
				//				val filteredRecords = jsonDataFrame.filter(jsonDataFrame("nname") === "Andy")
				//				filteredRecords.show()

	}
}