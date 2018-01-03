package com.ez.spark.dframes.operations

import org.apache.spark.sql._

object joins {
	def main (args: Array[String]) {
		val sparkSession = SparkSession.builder.master("local").getOrCreate()
				sparkSession.sparkContext.setLogLevel("ERROR")
				import sparkSession.implicits._

				val customersInfo = sparkSession.read
				.format("com.databricks.spark.csv")
				.option("delimiter", "\t")
				.option("header", "true")
        .load("src/main/resources/input/customers.txt")
				
        val ordersInfo = sparkSession.read
				.format("com.databricks.spark.csv")
				.option("delimiter", "\t")
				.option("header", "true")
        .load("src/main/resources/input/orders.txt")
        
				customersInfo.createOrReplaceTempView("Customers")
				ordersInfo.createOrReplaceTempView("Orders")
        
        val sqlDataFrame = sparkSession
				.sql("""SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate
                FROM Orders
                INNER JOIN Customers ON Orders.CustomerID=Customers.CustomerID""")
				//.sql("SELECT CustomerName FROM Customers UNION SELECT OrderID FROM Orders")
        sqlDataFrame.show()

	}
}