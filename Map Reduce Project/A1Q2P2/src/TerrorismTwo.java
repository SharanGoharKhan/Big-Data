import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TerrorismTwo {
	public static class TokenizerMapper
    extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable year = new IntWritable(1);
		private final static IntWritable country = new IntWritable(1);
		private final static IntWritable target = new IntWritable(1);
		private final static IntWritable weapon = new IntWritable(1);
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
		 public void map(Object key, Text value, Context context
		                 ) throws IOException, InterruptedException {
			 final String line = value.toString();
			 final String[] data = line.trim().split(","); 
		   if (line != null) {
			   String temp = "iyear";
			   if(data[1].trim().toString().equals(temp))
			   {
				   for(Integer i=0;i<data.length;++i)
				   {
					   String temp1;
					   temp1= i.toString()+"\t"+data[i];
					   System.out.println(temp1);
				   }
			   }
			   else {
				   String res = "YEAR: ";
				   res+=data[1];
				   word.set(res); // Year
				   context.write(word, year);
				   res="COUNTRY: ";
				   res+=data[8];
				   word.set(res);//country
				   context.write(word, country);
				   res="TARGET: ";
				   res+=data[40];
				   word.set(res);//target
				   context.write(word, target);
				   res="WEAPON: ";
				   res+=data[81];
				   word.set(res);//weapon
				   context.write(word, weapon);
				   
			   }
		   }
		 }
		}
	public static class IntSumReducer
	 extends Reducer<Text,IntWritable,Text,IntWritable> {
	 private IntWritable result = new IntWritable();
	
	 public void reduce(Text key, Iterable<IntWritable> values,
	                    Context context
	                    ) throws IOException, InterruptedException {
	   int sum = 0;
	   for (IntWritable val : values) {
	     sum += val.get();
	   }
	   result.set(sum);
	   context.write(key, result);
	 }
	}
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(TerrorismTwo.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
