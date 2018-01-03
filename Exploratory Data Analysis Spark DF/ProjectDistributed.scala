
import org.apache.spark.sql._
import org.apache.spark.sql
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object ProjectDistributed {
  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder.master("local").getOrCreate()
    import sparkSession.implicits._
    sparkSession.sparkContext.setLogLevel("ERROR")

    //From a textfile, same cane be done for other formats
    val eventsInfo = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("/home/sharan/spark work/football-events/events.csv")
    val metadataInfo = sparkSession.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .load("/home/sharan/spark work/football-events/ginf.csv")
    eventsInfo.createOrReplaceTempView("Events")
    metadataInfo.createOrReplaceTempView("Metadata")
    
    
    //Number of fouls Messi vs Ronaldo
    val foulCount = sparkSession.sql(
        """SELECT player, COUNT(player) AS Fouls FROM Events
          WHERE (player IN ('cristiano ronaldo','lionel messi')) 
          AND event_type = 3 GROUP BY player""" )
     foulCount.show();
          
          
    //Does having home ground have an advantage?
    val homeAdvantage = sparkSession.sql("""
      SELECT id_odsp AS homeId_odsp,side AS homeSide,event_team AS homeEvent_team,opponent AS homeOpponent,COUNT(is_goal) As homeGoals
      FROM Events where is_goal = 1 AND side = 1
      GROUP BY id_odsp,side, event_team,opponent
      order by id_odsp
      """)
    homeAdvantage.createOrReplaceTempView("HomeAdvantage")
   val AwayAdvantage = sparkSession.sql("""
     SELECT id_odsp AS awayId_odsp,side AS awaySide,event_team AS awayEvent_team,opponent AS awayOpponent,COUNT(is_goal) As awayGoals
      FROM Events where is_goal = 1 AND side = 2
      GROUP BY id_odsp,side,event_team,opponent
      order by id_odsp
     """)
     AwayAdvantage.createOrReplaceTempView("AwayAdvantage")
    val joinedAdvantage = sparkSession.sql("""
      SELECT *
      FROM HomeAdvantage JOIN AwayAdvantage
      ON HomeAdvantage.homeId_odsp = AwayAdvantage.awayId_odsp
      """)
      
      val advantageResult = joinedAdvantage.map(line=>
        if(line(4).toString().toInt > line(9).toString().toInt)
          {
          ("YES")
          }
        else{
          ("NO")
        }
        )
        val totalCount = advantageResult.count()
        val homeHasAdvantage = advantageResult.filter($"value"==="YES").count()
        println("Home has "+(homeHasAdvantage.toFloat*100/totalCount).toString()+"% "+"Advantage")
        
      
    //At which top 10 times are the most Yellow Cards shown?
    val yellowCard = sparkSession.sql(
        """
          SELECT time, COUNT(time) AS NumberOfYellowCards FROM Events
          WHERE event_type = 4
          GROUP BY time 
          ORDER BY COUNT(time) DESC LIMIT 10  
          """)
    yellowCard.show()
          
          
    //Successful shots Messi vs Ronaldo(Shots on target rate)
    val shotPlaceSuccessRonaldo = eventsInfo.filter($"player"==="cristiano ronaldo" && (($"shot_place">3 && $"shot_place"<5) || ($"shot_place" >11 && $"shot_place" <13)))
    val shotPlaceFailedRonaldo = eventsInfo.filter($"player" ==="cristiano ronaldo" && (($"shot_place">1 && $"shot_place"<2) || ($"shot_place" >6 && $"shot_place" <10)))
    val shotResultRonaldo = shotPlaceSuccessRonaldo.count().toFloat/shotPlaceFailedRonaldo.count()
    val shotPlaceSuccessMessi = eventsInfo.filter($"player"==="lionel messi" && (($"shot_place">3 && $"shot_place"<5) || ($"shot_place" >11 && $"shot_place" <13)))
    val shotPlaceFailedMessi = eventsInfo.filter($"player" ==="lionel messi" && (($"shot_place">1 && $"shot_place"<2) || ($"shot_place" >6 && $"shot_place" <10)))
    val shotResultMessi = shotPlaceSuccessMessi.count().toFloat/shotPlaceFailedMessi.count()   
    val resultTuples = List(("cristiano ronaldo", (shotResultRonaldo*100).toString()+"%"),("lionel messi",(shotResultMessi*100).toString()+"%"))
    import sparkSession.implicits._
    val tuplesCollectionDF = resultTuples.toDF("Player","Shot success rate")
    tuplesCollectionDF.show()
    
    
    //Which players has scored most goals by country
    val joinedRecords = sparkSession.sql(
        """
        Select Events.player, Metadata.country, Events.is_goal
				FROM Events  JOIN Metadata 
				ON Events.id_odsp = Metadata.id_odsp 
				WHERE Events.is_goal = 1
        """)
    joinedRecords.createOrReplaceTempView("JoinedRecords")
    val cnts = joinedRecords.groupBy($"country",$"player").count()
    cnts.createOrReplaceTempView("GoalsView")
    val res = sparkSession.sql("""
      SELECT country, player, count AS GoalCounts
      FROM GoalsView
      ORDER BY  GoalCounts DESC
      """)
    res.show()
      
      
      //Average number of goals in a game
    val avrgGoals = sparkSession.sql("""
      SELECT id_odsp, COUNT(is_goal) as Goals 
      FROM Events where is_goal = 1
      GROUP BY id_odsp
      """)
    avrgGoals.show()
    avrgGoals.createOrReplaceTempView("AvrgGoals")
    val avrgResult = sparkSession.sql("""
      SELECT AVG(Goals) AS AverageGoals
      FROM AvrgGoals
      """)
    avrgResult.show()
      
      
    //At which times are the highest number of goals made
      val timeGoal = sparkSession.sql("""
        SELECT time, COUNT(is_goal) AS Goals
        FROM Events where is_goal = 1
        GROUP BY time
        ORDER BY Goals DESC
        LIMIT 10
        """)
     timeGoal.show()
        
        
     //Which country has played most amount of games
      val countryGames = sparkSession.sql("""
        SELECT country, COUNT(country) AS GamesPlayed
        From Metadata
        GROUP BY country
        ORDER BY GamesPlayed DESC
        """)
      countryGames.show()
      
    
  }
}