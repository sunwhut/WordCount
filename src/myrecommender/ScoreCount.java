package myrecommender;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ScoreCount {
	public static class ScoreMap extends Mapper<LongWritable,Text,Text,IntWritable>{
		public void map(LongWritable key,Text value,Context context) 
				throws IOException, InterruptedException{
			String text = value.toString();
			String arrs[]= text.split("\n");
			for(String str:arrs){
				String[] arrs2 = str.split(" ");
				String strName = arrs2[0];
				String strScore = arrs2[1];
				Text name = new Text(strName);
				int score = Integer.parseInt(strScore);
				context.write(name, new IntWritable(score));
			}
		} 
	}
   
	public static class ScoreReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key,Iterable<IntWritable> values,Context context) 
				throws IOException, InterruptedException{
			int sum =0;
			int count =0;
			for(IntWritable iter:values){
				sum += iter.get();
				count++;
			}
			int average = (int) sum/count;
			context.write(key, new IntWritable(average));
		}
	}
	
	public static void main(String args[]) throws Exception{
		Configuration conf = new Configuration();
		String[] ioArgs = new String[] { "hdfs://localhost:9000/score/in",
										 "hdfs://localhost:9000/score/out" };
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
		if(otherArgs.length != 2){
			System.err.println("Usage: Score Average <in> <out>");
            System.exit(2);
		}
		Job job = new Job(conf, "scoreaverage");
		job.setJarByClass(ScoreCount.class);
		job.setMapperClass(ScoreMap.class);
		job.setCombinerClass(ScoreReduce.class);
		job.setReducerClass(ScoreReduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); 
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
