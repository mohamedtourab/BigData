package it.polito.bigdata.spark.exercise35;

import org.apache.spark.sql.Dataset;
import static org.apache.spark.sql.functions.max;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a Spark Session object and set the name of the application
		SparkSession ss = SparkSession.builder().appName("Spark Exercise #35 - DataFrame").getOrCreate();

		// Read the content of the input file and store it into a DataFrame
		// Meaning of the columns of the input file: sensorId,date,PM10 value
		// (μg/m3 )\n
		// The input file has no header. Hence, the name of the columns of
		// DataFrame will be _c0, _c1, _c2
		Dataset<Row> dfReadings = ss.read().format("csv").option("header", false).option("inferSchema", true)
				.load(inputPath);

		// Apply the max aggregate function over the values of the third column
		// of the dfReadings DataFrame
		Dataset<Row> maxValueDF = dfReadings.agg(max("_c2"));

		// maxValueDF contains only one Row with a field called max(c_2).
		// Select it by using the first action
		Row rowMaxValue = maxValueDF.first();
		// Retrieve the value of the column "max(_c2)" from the selected Row object
		Double maxValue= (Double) rowMaxValue.getAs("max(_c2)");

		// Filter the content of dsReadings
		// Select only the line(s) associated with the maximum value (maxValue)
		// and only the column associated with the date (i.e., _c1)
		Dataset<Row> selectedReadings =  dfReadings.filter("_c2="+maxValue).select("_c1");
		
		// Store the result in the output folder
		selectedReadings.write().format("csv").save(outputPath);

		// Close the Spark context
		ss.stop();
	}
}
