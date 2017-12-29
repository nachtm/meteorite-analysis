package com.nachtm.clustering;

import org.junit.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import java.io.IOException;
import java.util.Arrays;
public class RandomSelectionReducerTest{
	
	@Test
	public void outputsSingleValue() throws IOException {
		ReduceDriver<LongWritable, Text, LongWritable, Text> rd = new ReduceDriver<>();

		rd.getConfiguration().setBoolean(RandomSelectionMapper.DEBUG, true);

		rd.withReducer(new RandomSelectionReducer())
		  .withInput(RandomSelectionMapper.ONLY_KEY, Arrays.asList(
		  		new Text("1 Hi"),
		  		new Text("2 Bye")))
		  .withOutput(new LongWritable(1), new Text("Hi"))
		  .withOutput(new LongWritable(2), new Text("Bye"))
		  .runTest();
	}
}