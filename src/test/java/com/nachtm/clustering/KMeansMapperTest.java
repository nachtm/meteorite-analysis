package com.nachtm.clustering;

import org.junit.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

import java.io.IOException;

import com.nachtm.meteoriteanalysis.Geolocation;

public class KMeansMapperTest{

	private String centroids;

	@Before
	public void setup(){
		StringBuilder sb = new StringBuilder();
		sb.append("IDHERE ");
		sb.append(new Geolocation(-10, -10).toString());
		sb.append("\n");
		sb.append("IDHERE ");
		sb.append(new Geolocation(10, -10).toString());
		sb.append("\n");
		sb.append("IDHERE ");
		sb.append(new Geolocation(-10, 10).toString());
		sb.append("\n");
		sb.append("IDHERE ");
		sb.append(new Geolocation(10, 10).toString());
		sb.append("\n");
		sb.append("IDHERE ");
		sb.append(new Geolocation(-20, 10).toString());
		sb.append("\n");
		sb.append("IDHERE ");
		sb.append(new Geolocation(-10, 30).toString());
		sb.append("\n");
		centroids = sb.toString();
	}

	@Test
	public void outputsNearestCentroid() throws IOException{

		MapDriver<LongWritable, Text, Geolocation, Geolocation> md = new MapDriver<>();

		md.getConfiguration().setBoolean(KMeansMapper.DEBUG, true);
		md.getConfiguration().set(KMeansMapper.CENTROIDS, centroids);

		md.withMapper(new KMeansMapper())
		  .withInput(new LongWritable(1), new Text(new Geolocation(-11, -10).toString()))
		  .withInput(new LongWritable(2), new Text(new Geolocation(-11, 32).toString()))
		  .withOutput(new Geolocation(-10, -10), new Geolocation(-11, -10))
		  .withOutput(new Geolocation(-10, 30), new Geolocation(-11, 32))
		  .runTest();
	}
}