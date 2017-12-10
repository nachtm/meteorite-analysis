package com.nachtm.meteoriteanalysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.StringReader;

public class NumByAreaMapper 
		extends Mapper<LongWritable, Text, Geolocation, IntWritable> {

	private static final int LAT_IND = 7;
	private static final int LNG_IND = 8;
	private static final int BOX_WID = 5;
	private static final int BOX_HGT = 5;
	private static final IntWritable ONE = new IntWritable(1);

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		CSVParser parser = new CSVParser(new StringReader(value.toString()), CSVFormat.DEFAULT);
		CSVRecord line = parser.getRecords().get(0);
		String latStr = line.get(LAT_IND);
		String lngStr = line.get(LNG_IND);

		if(isValidLoc(latStr) && isValidLoc(lngStr)){
			double lat = Double.parseDouble(latStr);
			double lng = Double.parseDouble(lngStr);

			//create a 5deg x 5deg box to put all the collisions in
			double boxLat = nearestMultiple(lat, BOX_HGT);
			double boxLng = nearestMultiple(lng, BOX_WID);

			context.write(new Geolocation(boxLat, boxLng), ONE);
		}
	}

	private double nearestMultiple(double orig, int factor){
		return factor * (Math.round(orig / factor));
	}

	public static boolean isValidLoc(String val){
		//if string is empty or contains non-numeric (0-9 and .), it isn't valid
		for(int i = 0; i < val.length(); i++){
			char c = val.charAt(i);
			if(!(Character.isDigit(c) || c == '.' || c == '-')){
				return false;
			}
		}
		return val.length() > 0;
	}
}