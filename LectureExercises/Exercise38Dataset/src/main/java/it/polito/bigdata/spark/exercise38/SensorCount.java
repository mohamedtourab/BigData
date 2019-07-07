package it.polito.bigdata.spark.exercise38;

import java.io.Serializable;

@SuppressWarnings("serial")
public class SensorCount implements Serializable {

	private String sensorid;
	private double count;

	public String getSensorid() {
		return sensorid;
	}

	public void setSensorid(String sensorid) {
		this.sensorid = sensorid;
	}

	public double getCount() {
		return count;
	}

	public void setCount(double count) {
		this.count = count;
	}

}
