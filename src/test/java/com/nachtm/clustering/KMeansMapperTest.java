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
		sb.append("ID HERE,");
		sb.append(new Geolocation(-10, -10).toString());
		sb.append("\n");
		sb.append("ID HERE,");
		sb.append(new Geolocation(10, -10).toString());
		sb.append("\n");
		sb.append("ID HERE,");
		sb.append(new Geolocation(-10, 10).toString());
		sb.append("\n");
		sb.append("ID HERE,");
		sb.append(new Geolocation(10, 10).toString());
		sb.append("\n");
		sb.append("ID HERE,");
		sb.append(new Geolocation(-20, 10).toString());
		sb.append("\n");
		sb.append("ID HERE,");
		sb.append(new Geolocation(-10, 30).toString());
		sb.append("\n");
		centroids = sb.toString();
	}

	@Test
	public void outputsNearestCentroid() throws IOException{

		MapDriver<LongWritable, Geolocation, Geolocation, Geolocation> md = new MapDriver<>();

		md.getConfiguration().setBoolean(KMeansMapper.DEBUG, true);
		md.getConfiguration().set(KMeansMapper.CENTROIDS, centroids);

		md.withMapper(new KMeansMapper())
		  .withInput(new LongWritable(1), new Geolocation(-11, -10))
		  .withInput(new LongWritable(2), new Geolocation(-11, 32))
		  .withOutput(new Geolocation(-10, -10), new Geolocation(-11, -10))
		  .withOutput(new Geolocation(-10, 30), new Geolocation(-11, 32))
		  .runTest();
	}
}