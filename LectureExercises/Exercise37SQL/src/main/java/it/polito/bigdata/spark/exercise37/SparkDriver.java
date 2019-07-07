package it.polito.bigdata.spark.exercise37;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import it.polito.bigdata.spark.exercise37.Reading;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];


		// Create a Spark Session object and set the name of the application
		SparkSession ss = SparkSession.builder().appName("Spark Exercise #37 - SQL").getOrCreate();

		// Read the content of the input file and store it into a DataFrame
		// Meaning of the columns of the input file: sensorId,date,PM10 value
		// (μg/m3 )\n
		// The input file has no header. Hence, the name of the columns of
		// DataFrame will be _c0, _c1, _c2
		Dataset<Row> dfReadings = ss.read().format("csv").option("header", false).option("inferSchema", true)
				.load(inputPath);

		// Define a Dataset of Reading objects from the dfReading DataFrame
		Dataset<Reading> dsReadings = dfReadings.as(Encoders.bean(Reading.class));

		
		// Assign the “table name” readings to the dsReadings Dataset	
		dsReadings.createOrReplaceTempView("readings");

		// Select the maximum value of _c2 for each sensorid
		// by querying the readings table.
		// The result of the SQL-like query is a Dataset<Row>. 
		// The DataFrame is "casted" to a Dataset<SensorMax>
		Dataset<SensorMax> maxValuePerSensorDS = 
			ss.sql("SELECT _c0 as sensorid, max(_c2) as maxPM10 FROM readings GROUP BY _c0")
			  .as(Encoders.bean(SensorMax.class));

		// Store the result in the output folder
		maxValuePerSensorDS.write().format("csv").save(outputPath);

		// Close the Spark context
		ss.stop();
	}
}
