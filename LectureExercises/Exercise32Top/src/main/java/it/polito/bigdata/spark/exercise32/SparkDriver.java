package it.polito.bigdata.spark.exercise32;

import org.apache.spark.api.java.*;

import java.util.List;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;

		inputPath = args[0];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #32");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the input file
		JavaRDD<String> readingsRDD = sc.textFile(inputPath);

		// Extract the PM10 values
		// It can be implemented by using the map transformation
		JavaRDD<Double> pm10ValuesRDD = readingsRDD.map(PM10Reading -> {
			Double PM10value;

			// Split the line in fields
			String[] fields = PM10Reading.split(",");

			// fields[2] contains the PM10 value
			PM10value = new Double(fields[2]);
			return PM10value;
		});

		// Select the top-1 value
		List<Double> topPM10Value = pm10ValuesRDD.top(1);

		// Print the result on the standard output of the Driver
		// program
		for (Double value : topPM10Value) {
			System.out.println(value);
		}

		// Close the Spark context
		sc.close();
	}
}
