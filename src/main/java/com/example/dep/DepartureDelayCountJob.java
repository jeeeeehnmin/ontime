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
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

public class DepartureDelayCountJob {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		/*
		 * logback level 변경 : 편의상
		 */
		Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.INFO);
		
		Configuration conf = new Configuration();
		
		/*
		 * eclipse에서 hadoop을 바로 동작시키기 위한 설정
		 */
		conf.set("fs.defaultFS", "hdfs://bigdata01:8020");
		conf.set("yarn.resourcemanager.address", "bigdata01:8032");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.scheduler.address", "bigdata01:8030");
		
		Job job = new Job(conf, "DepartureDelayCount");
		
		/*
		 * setJar
		 * JobClient를 동작시키면, job에 대한 정보를 자동으로 hdfs에 전달해줌
		 *  --> 다른 클러스터 주체가 가져가서 사용함
		 * -- 없으면, 모든 노드매니저들에게 수동으로 알려줘야 한다
		 */
		job.setJar("target/ontime-0.0.1.jar");
//		job.setJarByClass(DepartureDelayCountJob.class);
		
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
