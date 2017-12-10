package com.nachtm.meteoriteanalysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class MaxMassReducer 
	extends Reducer<IntWritable, Text, IntWritable, Text>{

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		if(key.get() == 1){
			double maxMass = Double.MIN_VALUE;
			for(Text value : values) {
				double val = Double.parseDouble(value.toString());
				maxMass = Math.max(maxMass, val);
			}
			context.write(key, new Text(String.valueOf(maxMass)));
		} 
		// else{
		// 	StringBuilder sum = new StringBuilder();
		// 	for(Text value : values){
		// 		sum.append(value.toString());
		// 		sum.append("|\n");
		// 	}
		// 	context.write(key, new Text(sum.toString()));
		// }
	}
}
