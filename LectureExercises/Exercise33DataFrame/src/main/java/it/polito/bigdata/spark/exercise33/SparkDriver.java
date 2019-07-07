package it.polito.bigdata.spark.exercise33;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;

		inputPath = args[0];

		// Create a Spark Session object and set the name of the application
		SparkSession ss = SparkSession.builder().appName("Spark Exercise #33 - DataFrame").getOrCreate();

		// Read the content of the input file and store it into a DataFrame
		// Meaning of the columns of the input file: sensorId,date,PM10 value
		// (μg/m3 )\n
		// The input file has no header. Hence, the name of the columns of
		// DataFrame will be _c0, _c1, _c2
		Dataset<Row> dfReadings = ss.read().format("csv").option("header", false).option("inferSchema", true)
				.load(inputPath);

		// Select only column _c2, sort data by _c2 and finally select only the first 3 Row objects (i.e., the three 
		// maximum values of _c2
		// of the dsReadings DataFrame
		List<Row> maxValues = dfReadings.select("_c2").sort(new Column("_c2").desc()).takeAsList(3);
		   
		// Print the result on the standard output of the Driver
		// The name of the column is "_c2"
		
		for (Row r:maxValues) {
			System.out.println((Double) r.getAs("_c2"));
		}
		
		// Close the Spark context
		ss.stop();
	}
}
