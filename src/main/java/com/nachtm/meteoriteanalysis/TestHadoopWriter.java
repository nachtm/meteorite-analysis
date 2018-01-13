package com.nachtm.meteoriteanalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.io.OutputStream;
import java.io.IOException;

public class TestHadoopWriter{
	
	public static final String DEST_PATH = "test.txt";

	public static void write() throws IOException {

		String contents = "This is a drill.";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(DEST_PATH), conf);
		try(OutputStream out = fs.create(new Path(DEST_PATH))){
			out.write(contents.getBytes());
		}
	}

}