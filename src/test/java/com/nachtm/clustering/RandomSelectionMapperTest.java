package com.nachtm.clustering;

import org.junit.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

import java.io.IOException;

public class RandomSelectionMapperTest{
	
	@Test
	public void outputsSingleValue() throws IOException {
		Text input = new Text("Hello!");
		LongWritable lw = new LongWritable(42);

		MapDriver<LongWritable, Text, LongWritable, Text> md = new MapDriver<>();

		md.getConfiguration().setBoolean(RandomSelectionMapper.DEBUG, true);
		md.withMapper(new RandomSelectionMapper())
		  .withInput(lw, input)
		  .withOutput(RandomSelectionMapper.ONLY_KEY, new Text("" + lw + " " + input))
		  .runTest();

	}

	@Test
	public void outputsLargestValues() throws IOException {
		MapDriver<LongWritable, Text, LongWritable, Text> md = new MapDriver<>();

		md.getConfiguration().setBoolean(RandomSelectionMapper.DEBUG, true);
		md.getConfiguration().setInt(RandomSelectionMapper.NUM_TO_SELECT_PARAM, 2);

		md.withMapper(new RandomSelectionMapper())
		  .withInput(new LongWritable(1), new Text("No1"))
		  .withInput(new LongWritable(2), new Text("Yes2"))
		  .withInput(new LongWritable(3), new Text("Yes3"))
		  .withOutput(RandomSelectionMapper.ONLY_KEY, new Text("2 Yes2"))
		  .withOutput(RandomSelectionMapper.ONLY_KEY, new Text("3 Yes3"))
		  .runTest();
	}

	@Test
	public void canDoCompleteOverwrite() throws IOException {
		MapDriver<LongWritable, Text, LongWritable, Text> md = new MapDriver<>();

		md.getConfiguration().setBoolean(RandomSelectionMapper.DEBUG, true);
		md.getConfiguration().setInt(RandomSelectionMapper.NUM_TO_SELECT_PARAM, 3);

		md.withMapper(new RandomSelectionMapper())
		  .withInput(new LongWritable(1), new Text("No1"))
		  .withInput(new LongWritable(2), new Text("No2"))
		  .withInput(new LongWritable(3), new Text("No3"))
		  .withInput(new LongWritable(4), new Text("Yes4"))
		  .withInput(new LongWritable(5), new Text("Yes5"))
		  .withInput(new LongWritable(6), new Text("Yes6"))
		  .withOutput(RandomSelectionMapper.ONLY_KEY, new Text("4 Yes4"))
		  .withOutput(RandomSelectionMapper.ONLY_KEY, new Text("5 Yes5"))
		  .withOutput(RandomSelectionMapper.ONLY_KEY, new Text("6 Yes6"))
		  .runTest();

	}

	@Test
	public void noDuplicates() throws IOException {
		MapDriver<LongWritable, Text, LongWritable, Text> md = new MapDriver<>();
		
		md.getConfiguration().setBoolean(RandomSelectionMapper.DEBUG, true);
		md.getConfiguration().setInt(RandomSelectionMapper.NUM_TO_SELECT_PARAM, 2);

		md.withMapper(new RandomSelectionMapper())
		  .withInput(new LongWritable(1), new Text("Yes"))
		  .withInput(new LongWritable(2), new Text("Yes2"))
		  .withInput(new LongWritable(10000), new Text("Yes"))
		  .withOutput(RandomSelectionMapper.ONLY_KEY, new Text("1 Yes"))
		  .withOutput(RandomSelectionMapper.ONLY_KEY, new Text("2 Yes2"))
		  .runTest();
	}
}