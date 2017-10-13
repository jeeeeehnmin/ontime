package com.example.dep;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DepartureDelayCountJob {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
//		conf.set("fs.defaultFS", "hdfs://bigdata01:8020");
//		conf.set("yarn.resourcemanager.address", "bigdata01:8032");
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.scheduler.address", "bigdata01:8030");
		
		Job job = new Job(conf, "DepartureDelayCount");
		
		job.setJarByClass(DepartureDelayCountJob.class);
		
		FileInputFormat.setInputPaths(job, "dataexpo/1987_nohead.csv");
		FileInputFormat.addInputPaths(job, "dataexpo/1988_nohead.csv");
//		FileInputFormat.addInputPaths(job, "dataexpo/1989_nohead.csv");
//		FileInputFormat.addInputPaths(job, "dataexpo/1990_nohead.csv");
		
//		FileInputFormat.addInputPaths(job, "dataexpo");
		
		job.setInputFormatClass(TextInputFormat.class);	// 생략가능
		
		job.setMapperClass(DepartureDelayCountMapper.class);
		job.setMapOutputKeyClass(Text.class);				// 생략가능
		job.setMapOutputValueClass(IntWritable.class);	// 생략가능
		
		job.setReducerClass(DepartureDelayCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);// 생략가능
		
		String outputDir = "dataexpo_out/1988";
//		String outputDir = "dataexpo_out/1990";
//		String outputDir = "dataexpo_out/total";
		
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(new Path(outputDir), true);
		
		job.waitForCompletion(true);

	}

}
