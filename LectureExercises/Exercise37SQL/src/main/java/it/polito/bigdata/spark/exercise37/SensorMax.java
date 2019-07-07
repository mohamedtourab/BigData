package it.polito.bigdata.spark.exercise37;

import java.io.Serializable;

@SuppressWarnings("serial")
public class SensorMax implements Serializable {

	private String sensorid;
	private double maxPM10;

	public String getSensorid() {
		return sensorid;
	}

	public void setSensorid(String sensorid) {
		this.sensorid = sensorid;
	}

	public double getMaxPM10() {
		return maxPM10;
	}

	public void setMaxPM10(double maxPM10) {
		this.maxPM10 = maxPM10;
	}

}
