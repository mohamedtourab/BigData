package it.polito.bigdata.spark.exercise33;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import org.apache.spark.sql.Column;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;

		inputPath = args[0];

		// Create a Spark Session object and set the name of the application
		SparkSession ss = SparkSession.builder().appName("Spark Exercise #33 - Dataset").getOrCreate();

		// Read the content of the input file and store it into a DataFrame
		// Meaning of the columns of the input file: sensorId,date,PM10 value
		// (μg/m3 )\n
		// The input file has no header. Hence, the name of the columns of
		// DataFrame will be _c0, _c1, _c2
		Dataset<Row> dfReadings = ss.read().format("csv").option("header", false).option("inferSchema", true)
				.load(inputPath);

		// Define a Dataset of Reading objects from the dfReading DataFrame
		Dataset<Reading> dsReadings = dfReadings.as(Encoders.bean(Reading.class));
		

		// Select only column _c2. The returned Dataset is a Dataset<PM10> of PM10 objects. 
		// The schema of the returned Dataset<PM10> is mp10: double  
		// first 3 Row objects (i.e., the three maximum values of _c2 of the dsReadings DataFrame)
		Dataset<PM10> pm10Values = dsReadings.map(r -> new PM10(r.get_c2()), Encoders.bean(PM10.class));

		List<Double> maxValues = pm10Values.sort(new Column("pm10").desc()).as(Encoders.DOUBLE()).takeAsList(3);

		// Print the result on the standard output of the Driver
		for (Double d : maxValues) {
			System.out.println(d);
		}  

		// Close the Spark context
		ss.stop();
	}
}
