package com.nachtm.clustering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Arrays;
import java.io.InputStream;
import java.net.URI;
import java.io.IOException;

import com.nachtm.meteoriteanalysis.Geolocation;

//parses the output of a job
public class OutputParser{

	//read output from the initial random selection of centroids	
	public static List<Geolocation> parseCentroidsFromFile(String path) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path + "/part-r-00000"), conf);

		try(InputStream tempFile = fs.open(new Path(path +"/part-r-00000"))){
			Scanner s = new Scanner(tempFile);
			return parseCentroidsFromScanner(s);			
		}
	}

	public static List<Geolocation> parseCentroidsFromScanner(Scanner s){
		List<Geolocation> toReturn = new ArrayList<>();
		while(s.hasNextLine()){
			String line = s.nextLine();

			//skip the random id
			Scanner lineReader = new Scanner(line);
			lineReader.next();

			toReturn.add(parseGeolocation(lineReader.next()));
		}
		return toReturn;
	}

	//generate geolocation from string of form
	// id,lat,lon
	private static Geolocation parseGeolocation(String s){
		String[] parts = s.split(",");
		return new Geolocation(Double.parseDouble(parts[1]), Double.parseDouble(parts[2]));
	}
}