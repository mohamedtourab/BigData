package it.polito.bigdata.spark.exercise34;

import java.io.Serializable;
import java.sql.Timestamp;

@SuppressWarnings("serial")
public class Reading implements Serializable {

	private String _c0;
	private Timestamp _c1;
	private double _c2;

	public String get_c0() {
		return _c0;
	}

	public void set_c0(String _c0) {
		this._c0 = _c0;
	}

	public Timestamp get_c1() {
		return _c1;
	}

	public void set_c1(Timestamp _c1) {
		this._c1 = _c1;
	}

	public double get_c2() {
		return _c2;
	}

	public void set_c2(double _c2) {
		this._c2 = _c2;
	}

}
