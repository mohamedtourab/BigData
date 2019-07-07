package it.polito.bigdata.spark.exercise36;

import java.io.Serializable;

@SuppressWarnings("serial")
public class SumCount implements Serializable {
	int count;
	double sum;

	public SumCount(double sumPM10, int numElements) {
		this.sum = sumPM10;
		this.count = numElements;
	}

	public void setCount(int value) {
		this.count = value;
	}

	public int getCount() {
		return this.count;
	}

	public void setSum(double value) {
		this.sum = value;
	}

	public double getSum() {
		return this.sum;
	}

	public String toString() {
		return new String("" + this.sum / (double) this.count);
	}

}
