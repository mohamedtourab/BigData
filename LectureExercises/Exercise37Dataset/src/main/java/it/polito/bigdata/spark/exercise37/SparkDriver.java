package it.polito.bigdata.spark.exercise37;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a Spark Session object and set the name of the application
		SparkSession ss = SparkSession.builder().appName("Spark Exercise #37 - Dataset").getOrCreate();

		// Read the content of the input file and store it into a DataFrame
		// Meaning of the columns of the input file: sensorId,date,PM10 value
		// (μg/m3 )\n
		// The input file has no header. Hence, the name of the columns of
		// DataFrame will be _c0, _c1, _c2
		Dataset<Row> dfReadings = ss.read().format("csv").option("header", false).option("inferSchema", true)
				.load(inputPath);

		// Define a Dataset of Reading objects from the dfReading DataFrame
		Dataset<Reading> dsReadings = dfReadings.as(Encoders.bean(Reading.class));

		// Group data by sensorid (column _c0)
		RelationalGroupedDataset rgdReadingsPerSensor = dsReadings.groupBy("_c0");

		// For each sensor, apply the max aggregate function over the values of
		// the third column of the dfReadingsPerSensor RelationalGroupedDataset.
		// Compute the max of _c2 for each group.
		// Cast the returned DataFrame to a Dataset<SensorMax>
		Dataset<SensorMax> maxValuePerSensorDS = rgdReadingsPerSensor.max("_c2")
				.withColumnRenamed("_c0", "sensorid")
				.withColumnRenamed("max(_c2)", "maxPM10")
				.as(Encoders.bean(SensorMax.class));

		// Store the result in the output folder
		maxValuePerSensorDS.write().format("csv").save(outputPath);

		// Close the Spark context
		ss.stop();
	}
}
