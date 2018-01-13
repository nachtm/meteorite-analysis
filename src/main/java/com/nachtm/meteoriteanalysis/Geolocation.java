package com.nachtm.meteoriteanalysis;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.DoubleWritable;

import java.io.DataInput;
import java.io.DataOutput;

import java.io.IOException;

public class Geolocation implements WritableComparable<Geolocation>{
	
	DoubleWritable latitude;
	DoubleWritable longitude;
	
	public Geolocation(){
		latitude = new DoubleWritable();
		longitude = new DoubleWritable();
	}

	public Geolocation(double latitude, double longitude){
		this.latitude = new DoubleWritable(latitude);
		this.longitude = new DoubleWritable(longitude);
	}

	public DoubleWritable getLatitude(){
		return latitude;
	}

	public DoubleWritable getLongitude(){
		return longitude;
	}

	//arbitrarily, we choose to sort on latitude first
	@Override
	public int compareTo(Geolocation o){
		int latComp = latitude.compareTo(o.getLatitude());
		if(latComp != 0){
			return latComp;
		} else{
			return longitude.compareTo(o.getLongitude());
		}
	}

	@Override
	public boolean equals(Object o){
		if(o instanceof Geolocation){
			return compareTo((Geolocation) o) == 0;
		}
		return false;
	}

	@Override
	public int hashCode(){
		return ((latitude.hashCode() * 11) + longitude.hashCode());
	}

	@Override
	public void write(DataOutput out) throws IOException{
		latitude.write(out);
		longitude.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException{
		latitude.readFields(in);
		longitude.readFields(in);
	}

	public static Geolocation read(DataInput in) throws IOException{
		Geolocation g = new Geolocation(0,0);
		g.readFields(in);
		return g;
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append(latitude);
		sb.append(",");
		sb.append(longitude);
		return sb.toString();
	}
}