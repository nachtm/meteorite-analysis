package com.nachtm.clustering;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.PriorityQueue;
import java.io.IOException;

public class RandomSelectionReducer
		extends Reducer<LongWritable, Text, LongWritable, Text>{

	// private static final int ERROR_COUNT_KEY = 1331;

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		int numToSelect = context.getConfiguration().getInt(RandomSelectionMapper.NUM_TO_SELECT_PARAM, 
															RandomSelectionMapper.DEFAULT_NUM_TO_SELECT);
		boolean debug = context.getConfiguration().getBoolean(RandomSelectionMapper.DEBUG, false);

		PriorityQueue<Pair> selection = new PriorityQueue<>(numToSelect, (a,b) -> a.getVal().compareTo(b.getVal()));
		
		int errCount = 0;

		for(Text value : values){
			String[] splitPair = null;
			try{
				splitPair = splitOnFirstSpace(value.toString());
			} catch (IllegalArgumentException e){
				System.err.println(e);
				errCount++;
				continue;
			}
			assert splitPair != null;
			Pair valuePair = new Pair(new LongWritable(Long.parseLong(splitPair[0])), new Text(splitPair[1]));
			if(selection.size() < numToSelect){
				selection.offer(valuePair);
			} else if(selection.peek().getVal().compareTo(valuePair.getVal()) < 0){
				selection.poll();
				selection.offer(valuePair);
			}
		}

		for(Pair pair : selection){
			context.write(pair.getVal(), pair.getContents());
		}
	}

	private String[] splitOnFirstSpace(String s){
		String[] arr = new String[2];
		int firstSpace = s.indexOf(' ');
		if(firstSpace < 1 || firstSpace > s.length() - 1){
			throw new IllegalArgumentException("Expected two arguments separated by a space, got: " + s);
		}
		arr[0] = s.substring(0, firstSpace);
		arr[1] = s.substring(firstSpace + 1, s.length());
		return arr;
	}
}