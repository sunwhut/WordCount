package myrecommender;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import myrecommender.ScoreCount.ScoreMap;
import myrecommender.ScoreCount.ScoreReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.mahout.math.VarLongWritable;

public class MyRecommender {
	
	public static void main(String args[]) throws Exception{
		Configuration conf = new Configuration();
		String[] ioArgs = new String[] { "hdfs://localhost:9000/test/input",
										 "hdfs://localhost:9000/test/output" };
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
		if(otherArgs.length != 2){
			System.err.println("Usage: Score Average <in> <out>");
            System.exit(2);
		}
		Job job = new Job(conf, "scoreaverage");
		job.setJarByClass(MyRecommender.class);
		job.setMapperClass(SourceDataToItemPrefsMapper.class);
//		job.setCombinerClass(SourceDataToUserVectorReducer.class);
//		job.setReducerClass(SourceDataToUserVectorReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); 

	}

}
