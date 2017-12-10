package com.nachtm.meteoriteanalysis;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;

import java.io.IOException;

public class Geolocation implements WritableComparable<Geolocation>{
	double latitude;
	double longitude;

	public Geolocation(double latitude, double longitude){
		this.latitude = latitude;
		this.longitude = longitude;
	}

	public double getLatitude(){
		return latitude;
	}

	public double getLongitude(){
		return longitude;
	}

	//arbitrarily, we choose to sort on latitude first
	@Override
	public int compareTo(Geolocation o){
		if (latitude == o.getLatitude()){
			if (longitude == o.getLongitude()){
				return 0;
			} 
			return longitude < o.getLongitude() ? -1 : 1;
		}
		return latitude < o.getLatitude() ? -1 : 1;
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
		return (int) ((latitude * 11) + longitude);
	}

	@Override
	public void write(DataOutput out) throws IOException{
		out.writeDouble(latitude);
		out.writeDouble(longitude);
	}

	@Override
	public void readFields(DataInput in) throws IOException{
		latitude = in.readDouble();
		longitude = in.readDouble();
	}

	public static Geolocation read(DataInput in) throws IOException{
		Geolocation g = new Geolocation(0,0);
		g.readFields(in);
		return g;
	}
}