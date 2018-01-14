package com.nachtm.clustering;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

import com.nachtm.meteoriteanalysis.Geolocation;


public class KMeansReducer extends Reducer<Geolocation, Geolocation, Geolocation, Text>{
	//reduce: update centroids
		// 			//in: <centroid, list<members>
		// 			//out: <updated_centroid, list<members>

	@Override
	public void reduce(Geolocation centroid, Iterable<Geolocation> members, Context context)
			throws IOException, InterruptedException{
		//calculate new centroid
		double avLat = 0;
		double avLon = 0;
		int numMembers = 0;
		StringBuilder memberString = new StringBuilder();

		for(Geolocation member : members){
			avLat = shiftAverage(avLat, member.getLatitude().get(), numMembers);
			avLon = shiftAverage(avLon, member.getLongitude().get(), numMembers);
			numMembers ++; 
			memberString.append(member.toString());
			memberString.append("/");
		}

		context.write(new Geolocation(avLat, avLon), new Text(memberString.toString()));
	}

	private double shiftAverage(double currAv, double newVal, int numOldVals){
		return ((currAv * numOldVals) + newVal) / (numOldVals + 1);
	}
}