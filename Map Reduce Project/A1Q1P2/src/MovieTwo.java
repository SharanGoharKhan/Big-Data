import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieTwo {
	public static class TokenizerMapper
    extends Mapper<Object, Text, Text, Text>{
		private boolean firstLine = true;
		private Text valueRating = new Text();
		private final static IntWritable genreValue = new IntWritable(1);
		private final static IntWritable two = new IntWritable(1);
		private Text word = new Text();
		 public static boolean isInteger(String s) {
			 try {
				  Integer.parseInt(s);
			  } catch(NumberFormatException e) {
				  return false;
			  } catch (NullPointerException e) {
				  return false;
			  }
			 return true;
		 }
//		 public static boolean isFloat(String s) {
//			 try {
//				 Float.parseFloat(s);
//			 } 
//		 }
		 public void map(Object key, Text value, Context context
		                 ) throws IOException, InterruptedException {
			 final String line = value.toString();
			 final String[] data = line.trim().split(",");
		   
		   if(line != null)
		   {
			   for(int i=0;i<data.length;++i)
			   {
				 if(i==9)// Generes
				 {
					 String temp = "genres";
					 if(data[i].toString().trim().equals(temp)) {
						 System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>");
						   System.out.println("It is a header mapper function");
						   System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>");
						 word.set(data[i]);
						 context.write(word,word);
					 } else {
						 word.set(data[i]);
						 valueRating.set(data[24]);//set imdb rating
						 context.write(word, valueRating);
					 }
				 }
				 if(i==1)//Director
				 {
					 String temp = "director_name";
					 if(data[i].toString().trim().equals(temp)) {
						 word.set(data[i]);
						 context.write(word, word);
					 } else {
						 word.set(data[i]);
						 valueRating.set(data[24]);
						 context.write(word, valueRating);
					 }
				 }
				 if(i==22)//Year
				 {
					 String temp = "title_year";
					 if(data[i].toString().trim().equals(temp)) {
						 word.set(data[i]);
						 context.write(word, word);
					 } else {
						 word.set(data[i]);
						 valueRating.set(data[24]);
						 context.write(word, valueRating);
					 }
				 }
				 if(i==20)//content rating
				 {
					 String temp = "content_rating";
					 if(data[i].toString().trim().equals(temp)) {
						 word.set(data[i]);
						 context.write(word, word);
					 } else {
						 word.set(data[i]);
						 valueRating.set(data[24]);
						 context.write(word, valueRating);
					 }
				 }
			   }   
		   }
		 }
		}
	public static class IntSumReducer
	 extends Reducer<Text,Text,Text,Text> {
	 private static boolean isFloat(String s) {
		 try {
			 Float.parseFloat(s);
		 } catch(NumberFormatException e) {
			 return false;
		 } catch(NullPointerException e) {
			 return false;
		 }
		 return true;
	 }
	 private Text resultString = new Text();
	 private Float result = 0.0f;
	 public void reduce(Text key, Iterable<Text> values,
	                    Context context
	                    ) throws IOException, InterruptedException {
	   boolean isHeader = false;
	   boolean isGenres = false;
	   boolean isDirector = false;
	   boolean isYear = false;
	   boolean isRating = false;
	   float sum = 0;
	   int totalNumber = 0;
	   for (Text val: values) {
		   ++totalNumber;
		   String genres = "genres";
		   String director = "director_name";
		   String year = "title_year";
		   String rating = "content_rating";
		   if(val.toString().trim().equals(genres) || val.toString().trim().equals(director) || val.toString().trim().equals(year) || val.toString().trim().equals(rating))
		   {
			   if(val.toString().trim().equals(genres))
				   isGenres = true;
			   if(val.toString().trim().equals(director))
				   isDirector = true;
			   if(val.toString().trim().equals(year))
				   isYear = true;
			   if(val.toString().trim().equals(rating))
				   isRating = true;
			   isHeader = true;
			   break;
		   } else {
			   if(isFloat(val.toString()))
				 sum = sum + Float.parseFloat(val.toString());
		   }
	   }
	   if(isHeader) {
		   System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>");
		   System.out.println("It is a header reducer function");
		   System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>");
		   result = 0.0f;
		   resultString.set("IMDB RATING");
		   if(isGenres)
		   {
			   key.set(">>>>>>>>>>>>>>>>>>>>>>>>>GENRES>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		   	   context.write(key, resultString);   
		   }
		   if(isDirector)
		   {
			   key.set(">>>>>>>>>>>>>>>>>>>>>>>>>DIRECTOR>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		   	   context.write(key, resultString);
		   }
		   if(isYear)
		   {
			   key.set(">>>>>>>>>>>>>>>>>>>>>>>>>YEAR>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		   	   context.write(key, resultString);
		   }
		   if(isRating)
		   {
			   key.set(">>>>>>>>>>>>>>>>>>>>>>>>>RATING>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		   	   context.write(key, resultString);
		   }
	   } else {
		   result = sum/totalNumber;
		   resultString.set(result.toString());
		   context.write(key,resultString);
	   }
	 }
	}
	public static void main(String[] args) throws Exception {
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>");
		System.out.println("Main function called");
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>");
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(MovieTwo.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
