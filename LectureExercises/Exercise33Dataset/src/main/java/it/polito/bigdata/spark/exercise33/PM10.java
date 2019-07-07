package it.polito.bigdata.spark.exercise33;

import java.io.Serializable;

public class PM10 implements Serializable {

	private static final long serialVersionUID = 1L;

	private Double pm10;

	public PM10(double pm10) {
		this.pm10 = pm10;
	}

	public Double getPm10() {
		return pm10;
	}

	public void setPm10(Double pm10) {
		this.pm10 = pm10;
	}
	
	public String toString() {
		return pm10.toString();
	}

}
