package com.nachtm.clustering;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.PriorityQueue;
import java.util.Random;

import java.io.IOException;

//randomly selects a number of elements from the dataset based on http://had00b.blogspot.com/2013/07/random-subset-in-mapreduce.html
//I think this needs some analysis to see at what point the birthday paradox becomes a problem.
public class RandomSelectionMapper 
		extends Mapper<LongWritable, Text, LongWritable, Text>{
	
	public static final String NUM_TO_SELECT_PARAM = "number to select";
	public static final String DEBUG = "Debug";
	public static final int DEFAULT_NUM_TO_SELECT = 10;
	public static final LongWritable ONLY_KEY = new LongWritable(1); //hacky way to ensure only one reducer

	private int numToSelect;
	private PriorityQueue<Pair> selection;
	private boolean debug;
	private Random rand;

	@Override
	public void setup(Context context){
		numToSelect = context.getConfiguration().getInt(NUM_TO_SELECT_PARAM, DEFAULT_NUM_TO_SELECT);
		debug = context.getConfiguration().getBoolean(DEBUG, false);
		selection = new PriorityQueue<>(numToSelect, (a,b) -> a.getVal().compareTo(b.getVal()));
		rand = new Random();
	}

	//use a min-heap to keep track of the numToSelect largest elements we've "found" so far
	//by using a min-heap, we can easily tell if the number we currently have is larger than 
	//the smallest of the largest values and therefore belongs 
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		LongWritable newID = debug ? key : new LongWritable(rand.nextLong());
		Pair toAdd = new Pair(newID, value);
		if(selection.size() < numToSelect){
			selection.offer(toAdd);
			// if(!selection.add(toAdd)){
			// 	throw new IllegalStateException("Failure to add");
			// }
			// System.out.println("Added " + toAdd);
		} else if(selection.peek().getVal().compareTo(newID) < 0){
			// System.out.println("Removed " + selection.poll());
			// if(!selection.offer(toAdd)){
			// 	throw new IllegalStateException("Failure to replace");
			// }

			// System.out.println("Replaced with " + toAdd);
			// Pair removed = selection.remove();
			// selection.add(toAdd);
			// System.out.println("Replaced " + removed + " with " + toAdd);
			// System.out.println("Same contents: " + removed.getContents().toString().equals(toAdd.getContents().toString()));
			selection.poll();
			selection.offer(toAdd);
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		// System.out.println("starting cleanup");
		// System.out.println("Selection empty, size " + selection.isEmpty() + ", " + selection.size());
		for(Pair toEmit : selection){
			// System.out.println("Emitting " + toEmit);
			// if(toEmit.getContents().toString().isEmpty()){
			// 	throw new IllegalArgumentException("Empty output value " + toEmit.getVal());
			// } 
			context.write(ONLY_KEY, new Text(toEmit.getVal() + " " + toEmit.getContents()));
		}
	}
}