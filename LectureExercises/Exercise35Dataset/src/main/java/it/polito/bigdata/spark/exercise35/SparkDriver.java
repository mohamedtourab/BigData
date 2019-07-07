package it.polito.bigdata.spark.exercise35;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.max;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a Spark Session object and set the name of the application
		SparkSession ss = SparkSession.builder().appName("Spark Exercise #35 - Dataset").getOrCreate();

		// Read the content of the input file and store it into a DataFrame
		// Meaning of the columns of the input file: sensorId,date,PM10 value
		// (μg/m3 )\n
		// The input file has no header. Hence, the name of the columns of
		// DataFrame will be _c0, _c1, _c2
		Dataset<Row> dfReadings = ss.read().format("csv").option("header", false).option("inferSchema", true)
				.load(inputPath);

		// Define a Dataset of Reading objects from the dfReading DataFrame
		Dataset<Reading> dsReadings = dfReadings.as(Encoders.bean(Reading.class));

		// Apply the max aggregate function over the values of the third column
		// of the dsReadings Dataset.
		// The result of agg(max("_c2") is a Dataset<Row>. Each Row object of the 
		// returned Dataset contains only the field "max(_c2)" that is a Double. 
		// The DataFrame is "casted" to a Dataset<Double>
		Dataset<Double> maxValueDF = dsReadings.agg(max("_c2")).as(Encoders.DOUBLE());

		// maxValueDF contains only one Row with a field called max(c_2).
		// Select it by using the first action
		Double maxValue = maxValueDF.first();

		
		// Filter the content of dsReadings
		// Select only the line(s) associated with the maximum value (maxValue)
		// and the date column (i.e., _c1)
		Dataset<DateVal> selectedReadings =  dsReadings.filter(r -> r.get_c2()==maxValue.doubleValue())
													   .map(r -> new DateVal(r.get_c1()), Encoders.bean(DateVal.class));
		
		// Store the result in the output folder
		selectedReadings.write().format("csv").save(outputPath);

		// Close the Spark context
		ss.stop();
	}
}
