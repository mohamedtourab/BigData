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
		JavaRDD<SumCount> pm10ValuesRDD = readingsRDD.map(PM10Reading -> {
			Double PM10value;

			// Split the line in fields
			String[] fields = PM10Reading.split(",");

			// fields[2] contains the PM10 value
			PM10value = new Double(fields[2]);
			return new SumCount(PM10value, 1);
		});

		// Compute the sum of the PM10 values by using the reduce
		// method
		SumCount total = pm10ValuesRDD
				.reduce((element1, element2) -> new SumCount(element1.getSum() + element2.getSum(),
						element1.getCount() + element2.getCount()));

		System.out.println("Average=" + total.getSum() / (double) total.getCount());

		// Close the Spark context
		sc.close();
	}
}
