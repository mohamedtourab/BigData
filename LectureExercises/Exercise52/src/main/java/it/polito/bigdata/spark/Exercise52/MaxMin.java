package it.polito.bigdata.spark.Exercise52;

import java.io.Serializable;

@SuppressWarnings("serial")
public class MaxMin implements Serializable {
	private double maxPrice;
	private double minPrice;

	public MaxMin(double maxPrice, double minPrice) {
		this.maxPrice = maxPrice;
		this.minPrice = minPrice;
	}

	public double getMaxPrice() {
		return maxPrice;
	}

	public void setMaxPrice(double maxPrice) {
		this.maxPrice = maxPrice;
	}

	public double getMinPrice() {
		return minPrice;
	}

	public void setMinPrice(double minPrice) {
		this.minPrice = minPrice;
	}

	public double computePercentageVariation() {
		return 100 * (maxPrice - minPrice) / maxPrice;
	}

	public String toString() {
		return new String("" + this.computePercentageVariation());
	}

}
