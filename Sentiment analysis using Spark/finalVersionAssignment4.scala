//Question 1
    val lines = sc.textFile("/media/sf_sharedfolderubuntu/sharedfolderubuntu/input/movies_modified.csv");
    val split_lines  = lines.map(line => {
      line.split(",")(3)
    })
   val result = split_lines.filter(line => 
      line!=null && !line.isEmpty() && line.toInt>100
      )
    println("count: "+result.count());

//Question 2


def matchRecord(lineOfArray:Seq[String],wordToMatch:Set[String]):Int =
lineOfArray.map(wordInArray => {
	if(wordToMatch.contains(wordInArray)) {
		1;
	}
	else {
	0;
	}
}).sum
val abcnews = sc.textFile("/media/sf_sharedfolderubuntu/sharedfolderubuntu/input/abcnews.csv");
val positives = sc.textFile("/media/sf_sharedfolderubuntu/sharedfolderubuntu/input/positives.txt");
val negatives = sc.textFile("/media/sf_sharedfolderubuntu/sharedfolderubuntu/input/negatives.txt");
val posSet = positives.collect().toSet;
val negSet = negatives.collect().toSet;
val headlinesFilter = abcnews.filter(data => data.split(",").length == 2)
val headlines = headlinesFilter.map(line => {
		line.toLowerCase.split(",")(1)
		})

val resultHeadlines = headlines.map(line => {
			val tokenizeLine = line.toLowerCase.split("\\W")
			(line, matchRecord(tokenizeLine,posSet), matchRecord(tokenizeLine,negSet))
			})
val finalResult = resultHeadlines.map(col => {
			if(col._2 > col._3) 
				(col._1,col._2,col._3,"positive")
			else if(col._2 < col._3)
				(col._1,col._2,col._3,"negative")
			else
				(col._1,col._2,col._3,"neutral")
		})
finalResult.saveAsTextFile("/home/sharan/spark/output/result");
