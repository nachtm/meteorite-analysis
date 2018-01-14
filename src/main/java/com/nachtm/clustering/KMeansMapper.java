package com.nachtm.clustering;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.List;
import java.io.IOException;
import java.util.Scanner;

import com.nachtm.meteoriteanalysis.Geolocation;

//reads in a list of centroids from a file. Then, for each input item, outputs <centroid, item>,
//where centroid is the nearest centroid.
public class KMeansMapper extends Mapper<LongWritable, Text, Geolocation, Geolocation>{

	public static final String READ_PATH = "read path";
	public static final String DEBUG = "debug";
	public static final String CENTROIDS = "centroids";
	private List<Geolocation> centroids;

	@Override
	public void setup(Context context){
		if(context.getConfiguration().getBoolean(DEBUG, false)){
			centroids = OutputParser.parseCentroidsFromScanner(new Scanner(context.getConfiguration().get(CENTROIDS)));  
		} else {
			String path = context.getConfiguration().get(READ_PATH);
			try{
				centroids = OutputParser.parseCentroidsFromFile(path);			
			} catch(IOException e){
				e.printStackTrace();
			}	
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Geolocation currDataPoint = OutputParser.parseGeolocation(value.toString());

		Geolocation nearestCentroid = centroids.get(0);
		double nearestDist = distance(currDataPoint, nearestCentroid);
		for(int i = 1; i < centroids.size(); i++){
			Geolocation currCentroid = centroids.get(i);
			double currDist = distance(currDataPoint, currCentroid);
			if(currDist < nearestDist){
				nearestDist = currDist;
				nearestCentroid = currCentroid;
			}

		}
		context.write(nearestCentroid, currDataPoint);
	}

	private double distance(Geolocation from, Geolocation to){
		double latDiff = from.getLatitude().get() - to.getLatitude().get();
		double lonDiff = from.getLongitude().get() - to.getLongitude().get();
		return Math.sqrt(Math.pow(latDiff, 2) + Math.pow(lonDiff, 2));
	}

		
}