package it.polito.bigdata.spark.exercise35;

import java.io.Serializable;
import java.sql.Timestamp;

@SuppressWarnings("serial")
public class DateVal implements Serializable {

	private Timestamp dateVal;

	public DateVal(Timestamp dateVal) {
		this.dateVal = dateVal;
	}

	public Timestamp getDateVal() {
		return dateVal;
	}

	public void setDateVal(Timestamp dateVal) {
		this.dateVal = dateVal;
	}
	
}
