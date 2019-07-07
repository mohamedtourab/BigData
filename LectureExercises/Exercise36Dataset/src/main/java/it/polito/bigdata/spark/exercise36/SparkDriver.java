package it.polito.bigdata.spark.exercise36;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.avg;

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

		// Apply the avg aggregate function over the values of the third column
		// of the dsReadings Dataset.
		// The result of agg(avg("_c2") is a Dataset<Row>. Each Row object of the 
		// returned Dataset contains only the field "avg(_c2)" that is a Double. 
		// The DataFrame is "casted" to a Dataset<Double>
		Dataset<Double> avgValueDF = dsReadings.agg(avg("_c2")).as(Encoders.DOUBLE());

		// maxValueDF contains only one Row with a field called avg(c_2).
		// Select it by using the first action
		Double avgValue = avgValueDF.first();

		// Print the result on the standard output of the Driver
		// The name of the column is "avg(_c2)"
		System.out.println(avgValue);

		// Close the Spark context
		ss.stop();
	}
}
