package com.nachtm.clustering;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Pair{

	private LongWritable val;
	private Text contents;
	private Text contentsCopy;

	Pair(LongWritable val, Text input){
		this.val = val;
		this.contents = new Text(input.toString());
		// this.contents = input;
		this.contentsCopy = input;
		if(input.toString().isEmpty()){
			throw new IllegalArgumentException("Empty contents");
		}
	}

	public LongWritable getVal(){
		return this.val;
	}

	public Text getContents(){
		// if(this.contentsCopy.toString().isEmpty()){
		// 	throw new IllegalStateException("copy is also empty :(");
		// }
		if(this.contents.toString().isEmpty()){
			throw new IllegalStateException("Accessing empty contents");
		}
		return this.contents;
	}

	@Override
	public String toString(){
		return getVal() + " " + getContents();
	}
}