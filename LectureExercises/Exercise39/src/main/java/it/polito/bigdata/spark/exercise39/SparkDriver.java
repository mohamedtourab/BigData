package it.polito.bigdata.spark.exercise39;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #39");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the input file
		JavaRDD<String> readingsRDD = sc.textFile(inputPath);

		// Apply a filter transformation to select only the lines with PM10>50
		JavaRDD<String> readingsHighValueRDD = readingsRDD.filter(PM10Reading -> {
			double PM10value;

			// Split the line in fields
			String[] fields = PM10Reading.split(",");

			// fields[2] contains the PM10 value
			PM10value = Double.parseDouble(fields[2]);

			if (PM10value > 50)
				return true;
			else
				return false;

		});

		// Create a PairRDD
		// Each pair contains a sensorId (key) and a date (value)
		// It can be implemented by using the mapToPair transformation
		JavaPairRDD<String, String> sensorsCriticalDatesRDD = readingsHighValueRDD.mapToPair(PM10Reading -> {

			String sensorID;
			String date;
			Tuple2<String, String> pair;

			// Split the line in fields
			String[] fields = PM10Reading.split(",");

			// fields[0] contains the sensorId
			sensorID = fields[0];

			// fields[1] contains the date
			date = fields[1];

			pair = new Tuple2<String, String>(sensorID, date);

			return pair;
		});

		// Create one pair for each sensor (key) with the list of
		// dates associated with that sensor (value)
		// by using the groupByKey transformation
		JavaPairRDD<String, Iterable<String>> finalSensorCriticalDates = sensorsCriticalDatesRDD.groupByKey();

		finalSensorCriticalDates.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
