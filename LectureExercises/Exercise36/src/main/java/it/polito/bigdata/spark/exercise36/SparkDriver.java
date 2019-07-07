package it.polito.bigdata.spark.exercise36;

import org.apache.spark.api.java.*;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;

		inputPath = args[0];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #36");

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

		// Compute the sum of the PM10 values by using the reduce
		// method
		Double sum = pm10ValuesRDD.reduce((Double element1, Double element2) -> new Double(element1 + element2));

		// Count the number of lines
		long numLines = pm10ValuesRDD.count();

		System.out.println("Average=" + sum / numLines);

		// Close the Spark context
		sc.close();
	}
}
