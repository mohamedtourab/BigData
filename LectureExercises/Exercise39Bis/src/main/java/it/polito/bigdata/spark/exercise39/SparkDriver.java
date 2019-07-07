package it.polito.bigdata.spark.exercise39;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #39"); //.setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the input file
		JavaRDD<String> readingsRDD = sc.textFile(inputPath);

		// Create a PairRDD
		// Each pair contains a sensorId (key) and a date (value). The empty string if
		// the PM10 value is <=50
		// It can be implemented by using the mapToPair transformation
		JavaPairRDD<String, String> sensorsCriticalDatesRDD = 
				readingsRDD.mapToPair(PM10Reading -> {

			String sensorID;
			String date;
			Tuple2<String, String> pair;

			// Split the line in fields
			String[] fields = PM10Reading.split(",");

			// fields[0] contains the sensorId
			sensorID = fields[0];

			// fields[1] contains the date
			date = fields[1];

			// fields[2] contains the PM10 value
			double PM10value = Double.parseDouble(fields[2]);

			if (PM10value > 50)
				pair = new Tuple2<String, String>(sensorID, date);
			else
				pair = new Tuple2<String, String>(sensorID, null);

			return pair;
		});

		// Create one pair for each sensor (key) with the list of
		// dates associated with that sensor (value)
		// by using the groupByKey transformation.
		// For those sensors that are never associated with a PM10 value greater than 50,
		// the associated list contains only empty strings
		JavaPairRDD<String, Iterable<String>> finalSensorCriticalDates 
		= sensorsCriticalDatesRDD.groupByKey();

		// Use mapValues to remove null strings from each list of dates
		JavaPairRDD<String, List<String>> finalResultWithoutEmptyStrings = finalSensorCriticalDates
				.mapValues((Iterable<String> listDates) -> {
					ArrayList<String> listWithoutEmptyStrings = 
							new ArrayList<String>();

					for (String date : listDates) {
						if (date!=null)
							listWithoutEmptyStrings.add(date);
					}

					return listWithoutEmptyStrings;
				});

		finalResultWithoutEmptyStrings.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
