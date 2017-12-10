package com.nachtm.meteoriteanalysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NumByAreaReducer 
		extends Reducer<Geolocation, IntWritable, Geolocation, IntWritable> {
	
	@Override
	public void reduce(Geolocation key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int count = 0;

		for(IntWritable val : values){
			count+= val.get();
		}
		context.write(key, new IntWritable(count));
	}			
	
}