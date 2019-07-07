package it.polito.bigdata.spark.exercise46;

import java.io.Serializable;

@SuppressWarnings("serial")
public class TimeStampTemperature implements Serializable {

	int timestamp; 
	double temperature;

	
	public TimeStampTemperature(int timestampValue, double temp) {
		this.timestamp=timestampValue;
		this.temperature=temp;
	}
	
	public void setTimestamp(int value) {
		this.timestamp=value;
	}
	
	public int getTimestamp() {
		return this.timestamp;
	}

	public void setTemperature(double value) {
		this.temperature=value;
	}

	public double getTemperature() {
		return this.temperature;
	}
	
	public String toString() {
		return new String(this.timestamp+","+this.temperature);
	}
}