package com.nachtm.clustering;

import com.nachtm.meteoriteanalysis.Geolocation;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;


import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Random;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import com.nachtm.meteoriteanalysis.Geolocation;

//For right now, assume we're clustering geolocations. 
//Later, we'll update this to be more generic.
public class KMeansClusteringDriver{
	
	static final String TMP_OUTPUT_PATH = "tmp";
	private Random rand = new Random();

	//thanks to http://had00b.blogspot.com/2013/07/random-subset-in-mapreduce.html for the insight
	public String randomCentroids(int numCentroids, Path input)
			throws IOException, InterruptedException {

		//setup and run the distributed job
		Path output = new Path(TMP_OUTPUT_PATH + rand.nextInt());

		Job job = Job.getInstance();
		job.getConfiguration().setBoolean(RandomSelectionMapper.DEBUG, false);
		job.setJarByClass(KMeansClusteringDriver.class);
		job.setJobName("Random Selection");

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.setMapperClass(RandomSelectionMapper.class);
		job.setReducerClass(RandomSelectionReducer.class);

		try{
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e){
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("Done");

		
		return output.toString();
	}

	private String runClusteringStep(String centroidFileLocation, Path input) 
			throws IOException, InterruptedException{
		String outputPath = TMP_OUTPUT_PATH + rand.nextInt();
		while(centroidFileLocation.equals(outputPath)){
			outputPath = TMP_OUTPUT_PATH + rand.nextInt();
		}
		runClusteringStep(centroidFileLocation, outputPath, input);
		return outputPath;
	}

	private static void runClusteringStep(String centroidFileLocation, String outputPath, Path input) 
			throws IOException, InterruptedException{

		Job job = Job.getInstance();
		job.getConfiguration().set(KMeansMapper.READ_PATH, centroidFileLocation);
		job.setJarByClass(KMeansClusteringDriver.class);
		job.setJobName("K means iteration");

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));


		job.setMapperClass(KMeansMapper.class);
		job.setReducerClass(KMeansReducer.class);

		job.setMapOutputKeyClass(Geolocation.class);
		job.setMapOutputValueClass(Geolocation.class);

		try{
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e){
			e.printStackTrace();
			System.exit(1);
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException{

		KMeansClusteringDriver kmcd = new KMeansClusteringDriver();
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);

		//Read in data
		//randomly select centroids
		Path dataPath = new Path("data/meteorite_landings_latlon_noempty.csv");
		String centroidFileLocation = kmcd.randomCentroids(10, dataPath);

		// for(Geolocation centroid : centroids){
		// 	System.out.println(centroid);
		// }


		//while convergence hasn't been reached:

		String membersFileLocation = "";
		for(int i = 0; i < 1; i++){
			if (!membersFileLocation.isEmpty() && hdfs.exists(new Path(membersFileLocation))) {
				hdfs.delete(new Path(membersFileLocation), true);
			}
			membersFileLocation = kmcd.runClusteringStep(centroidFileLocation, dataPath);

			//also update the centroids file

			System.out.println("Iteration number " + i + " complete.");
		}
		System.out.println("Done!");
	}
}



						//write centroids to a file

			//run one map-reduce job
				//map: update cluster assignment
					//in:  <X, member>
					//out: <centroid, List<members>
				//reduce: update centroids
					//in: <centroid, list<members>
					//out: <updated_centroid, list<members>
