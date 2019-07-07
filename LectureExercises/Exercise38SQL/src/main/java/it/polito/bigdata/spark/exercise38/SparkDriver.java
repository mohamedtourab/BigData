package it.polito.bigdata.spark.exercise38;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import it.polito.bigdata.spark.exercise38.Reading;

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

		// Select only the records with PM10>50 and 
		// Count for each sensor the number of records with PM10>50
		// Select the sensors with at least 2 readings with a
		// PM10 value greater than the critical threshold 50 and the number of critival readings
		// PM10 is column _c2
		// SensorId is column _c0
		// The result of the SQL-like query is a Dataset<Row>. Each Row object of the 
		// The DataFrame is "casted" to a Dataset<SensorCount>
		Dataset<SensorCount> maxValuePerSensorDS = 
			ss.sql("SELECT _c0 as sensorid, count(*) as count FROM readings WHERE _c2>50 GROUP BY _c0 HAVING count(*)>=2")
			  .as(Encoders.bean(SensorCount.class));

		// Store the result in the output folder
		maxValuePerSensorDS.write().format("csv").save(outputPath);

		// Close the Spark context
		ss.stop();
	}
}
