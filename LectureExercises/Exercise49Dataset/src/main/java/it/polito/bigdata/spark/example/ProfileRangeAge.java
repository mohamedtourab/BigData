package it.polito.bigdata.spark.example;

import java.io.Serializable;

@SuppressWarnings("serial")
public class ProfileRangeAge implements Serializable {
	private String name;
	private String surname;
	private String rangeage;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSurname() {
		return surname;
	}

	public void setSurname(String surname) {
		this.surname = surname;
	}

	public String getRangeage() {
		return rangeage;
	}

	public void setRangeage(String rangeage) {
		this.rangeage = rangeage;
	}

}
