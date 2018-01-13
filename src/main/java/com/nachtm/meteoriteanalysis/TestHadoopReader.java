package com.nachtm.meteoriteanalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.InputStream;
import java.io.IOException;
import java.net.URI;

public class TestHadoopReader{
	
	public static void read() throws IOException {
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(TestHadoopWriter.DEST_PATH), conf);
		
		String s = "";
		
		try(InputStream in = fs.open(new Path(TestHadoopWriter.DEST_PATH))){
			StringBuilder sb = new StringBuilder();
			byte[] nextBytes = new byte[4096];
			while(true){
				int numBytes = in.read(nextBytes);
				if(numBytes >= 0){
					sb.append(new String(nextBytes, 0, numBytes));
				} else{
					break;
				}
			}
			s = sb.toString();
		}
		System.out.println(s);
	}

	public static void main(String[] args) throws IOException {
		TestHadoopWriter.write();
		TestHadoopReader.read();
	}
}