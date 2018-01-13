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
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import com.nachtm.meteoriteanalysis.Geolocation;

//For right now, assume we're clustering geolocations. 
//Later, we'll update this to be more generic.
public class KMeansClusteringDriver{
	
	static final String TMP_OUTPUT_PATH = "tmp";


	//thanks to http://had00b.blogspot.com/2013/07/random-subset-in-mapreduce.html for the insight
	public static List<Geolocation> randomCentroids(int numCentroids, Path input)
			throws IOException, InterruptedException {

		//setup and run the distributed job
		Path output = new Path(TMP_OUTPUT_PATH);

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

		
		return OutputParser.parseCentroidsFromFile(TMP_OUTPUT_PATH);
	}

	public static void main(String[] args) throws IOException, InterruptedException{

		//Read in data
		//randomly select centroids
		List<Geolocation> centroids = randomCentroids(10, new Path("data/meteorite_landings_idlatlon_noempty.csv"));

		for(Geolocation centroid : centroids){
			System.out.println(centroid);
		}


		//while convergence hasn't been reached:

		int iters = 0;
		while(numIters < 20){
			//write centroids to a file
						
			//run one map-reduce job
				//map: update cluster assignment
					//in:  <X, member>
					//out: <centroid, List<members>
				//reduce: update centroids
					//in: <centroid, list<members>
					//out: <updated_centroid, list<members>

		}

	}
}