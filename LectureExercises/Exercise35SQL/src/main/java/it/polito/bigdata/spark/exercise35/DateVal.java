package it.polito.bigdata.spark.exercise35;

import java.io.Serializable;
import java.sql.Timestamp;

@SuppressWarnings("serial")
public class DateVal implements Serializable {

	private Timestamp _c1;

	public Timestamp get_c1() {
		return _c1;
	}

	public void set_c1(Timestamp _c1) {
		this._c1 = _c1;
	}
	
}
