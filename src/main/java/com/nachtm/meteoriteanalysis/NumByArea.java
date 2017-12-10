package com.nachtm.meteoriteanalysis;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.Path;

public class NumByArea{

	public static void main(String[] args) throws Exception {
		if(args.length != 2){
			System.out.println("Usage: NumByArea <infile> <outdir>");
			System.exit(1);
		}

		Job job = Job.getInstance();
		job.setJarByClass(NumByArea.class);
		job.setJobName("Heat Map Generation");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(NumByAreaMapper.class);
		job.setCombinerClass(NumByAreaReducer.class);
		job.setReducerClass(NumByAreaReducer.class);

		job.setOutputKeyClass(Geolocation.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	
}