package com.ez.spark.project

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

case class Q2Schema(id_event: String, time: Integer, event_team: String, is_goal: Integer)

object Basics {
  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder.master("local").getOrCreate()
    import sparkSession.implicits._
    sparkSession.sparkContext.setLogLevel("ERROR")

    //From a textfile, same cane be done for other formats
    val eventsInfo = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/main/resources/project/events.csv")

    //time at which most goals are scored
    //we need time and is_goal

    val reqRecords = eventsInfo.select("time", "is_goal")
    val filteredRecords = reqRecords.filter("time != 90 AND time != 45 AND is_goal == 1")
    val groupedReords = filteredRecords.groupBy("time")
    val maxTime = groupedReords.count().orderBy(desc("count"))
    //println(filteredRecords.count())

    eventsInfo.createOrReplaceTempView("Events")
    val sreqRecords = sparkSession.sql(
      """Select time, COUNT(time) AS goalCount FROM Events 
						WHERE time != 90 AND time != 45 AND is_goal = 1
						GROUP BY time ORDER BY COUNT(time) DESC""")
    //sreqRecords.show()

    //In which leagues is the referee more likely to give a card?
    val metadataInfo = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/main/resources/project/ginf.csv")

    metadataInfo.createOrReplaceTempView("Metadata")

    val joinedRecords = sparkSession
      .sql("""Select Events.event_type, Metadata.league
						FROM Events  JOIN Metadata  
						ON Events.id_odsp = Metadata.id_odsp """)

    //probably can be done by a single nested query
    joinedRecords.createOrReplaceTempView("JoinedRecords")

    val maxCards = sparkSession
      .sql("""Select league, Count(league)
						FROM JoinedRecords  
						WHERE event_type = 6
						GROUP BY league """)

    //maxCards.show()
						
		//Ronaldo vs Messi - total events 
		//not all of them goals, and some being negative like fouls
		 val ronaldoMessi1 = sparkSession.sql(
      """Select player, count(player) AS goalCount FROM Events 
						WHERE player = 'cristiano ronaldo' OR player = 'lionel messi'
						GROUP BY player""")		
			//ronaldoMessi1.show()	
			
		//Ronaldo vs Messi - total goals 
		val ronaldoMessi2 = sparkSession.sql(
      """Select player, count(player) AS goalCount FROM Events 
						WHERE (player IN ('cristiano ronaldo', 'lionel messi'))
						                AND is_goal = 1
						GROUP BY player""")
						
			//ronaldoMessi2.show()	
			
		//Ronaldo vs Messi - total goals and average of times
		val ronaldoMessi3 = sparkSession.sql(
      """Select player, count(player) AS goalCount, ROUND(AVG(CAST(time as DOUBLE)),2) as AverageTime 
            FROM Events WHERE 
						(player IN ('cristiano ronaldo', 'lionel messi')) AND is_goal = 1
						GROUP BY player""")
    
			//ronaldoMessi3.show()	
		
			//Ronaldo vs Messi - goals against each other in away games
		val ronaldoMessi4 = sparkSession.sql(
      """Select sub.player, count(sub.player) AS goalCount 
            FROM (Select * from Events where opponent IN ('Real Madrid', 'Barcelona')) sub 
            WHERE 
						(player IN ('cristiano ronaldo', 'lionel messi')) AND is_goal = 1
						GROUP BY player""")
    
						
			ronaldoMessi4.show()	
			
			val recordsToWrite = ronaldoMessi4.coalesce(1)
			recordsToWrite.write
			.format("com.databricks.spark.csv")
			.option("header", "true")
			.save("src/main/resources/projectOut")
			
			
  }
}