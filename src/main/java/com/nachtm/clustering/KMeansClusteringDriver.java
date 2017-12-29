package com.nachtm.clustering;

import com.nachtm.meteoriteanalysis.Geolocation;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
//For right now, assume we're clustering geolocations. 
//Later, we'll update this to be more generic.
public class KMeansClusteringDriver{
	
	//thanks to http://had00b.blogspot.com/2013/07/random-subset-in-mapreduce.html for the insight
	public static List<Geolocation> randomCentroids(int numCentroids, Path input)
			throws IOException, InterruptedException {
		
		//read output

		return new ArrayList<>();
	}
	public static void main(String[] args) throws IOException, InterruptedException{

		//Read in data
		//randomly select centroids
		Path output = new Path("output");

		Job job = Job.getInstance();
		job.getConfiguration().setBoolean(RandomSelectionMapper.DEBUG, false);
		job.setJarByClass(KMeansClusteringDriver.class);
		job.setJobName("Random Selection");

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, output);

		job.setMapperClass(RandomSelectionMapper.class);
		job.setReducerClass(RandomSelectionReducer.class);

		// job.setOutputKeyClass(LongWritable.class);
		// job.setOutputValueClass(Text.class);
		try{
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e){
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("Done");

		//while convergence hasn't been reached:
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