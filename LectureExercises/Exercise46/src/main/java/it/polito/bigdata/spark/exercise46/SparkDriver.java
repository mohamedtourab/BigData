package it.polito.bigdata.spark.exercise46;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.spark.SparkConf;

public class SparkDriver {

	@SuppressWarnings("resource")
	public static void main(String[] args) {

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #46");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the readings
		JavaRDD<String> readingsRDD = sc.textFile(inputPath);

		// Generate the elements of each window.
		// Each reading with start time t belongs to 3 windows
		// with a window size equal to 3:
		// - The one starting at time t-120s
		// - The one starting at time t-60s
		// - The one starting at time t
		JavaPairRDD<Integer, TimeStampTemperature> windowsElementsRDD = readingsRDD.flatMapToPair(reading -> {

			// ArrayList stores locally the three elements
			// (window start timestamp, current reading) associated with
			// the three windows containing this reading
			// TimeStampTemperature is a class that can be used to store a time
			// stamp and the associated
			// temperature value (i.e., TimeStampTemperature contains one
			// reading
			ArrayList<Tuple2<Integer, TimeStampTemperature>> pairs = new ArrayList<Tuple2<Integer, TimeStampTemperature>>();

			Tuple2<Integer, TimeStampTemperature> pair;

			String[] fields = reading.split(",");

			// fields[0] = time stamp
			// fields[1] = temperature

			// The current reading, associated with time stamp t,
			// is part of the windows starting at time t, t-60s, t-120s

			// Window starting at time t
			pair = new Tuple2<Integer, TimeStampTemperature>(Integer.parseInt(fields[0]),
					new TimeStampTemperature(Integer.parseInt(fields[0]), Double.parseDouble(fields[1])));
			pairs.add(pair);

			// Window starting at time t-60s
			pair = new Tuple2<Integer, TimeStampTemperature>(Integer.parseInt(fields[0]) - 60,
					new TimeStampTemperature(Integer.parseInt(fields[0]), Double.parseDouble(fields[1])));
			pairs.add(pair);

			// Window starting at time t-120s
			pair = new Tuple2<Integer, TimeStampTemperature>(Integer.parseInt(fields[0]) - 120,
					new TimeStampTemperature(Integer.parseInt(fields[0]), Double.parseDouble(fields[1])));
			pairs.add(pair);

			return pairs.iterator();
		});

		// Use groupbykey to generate one sequence for each time stamp
		JavaPairRDD<Integer, Iterable<TimeStampTemperature>> timestampsWindowsRDD = windowsElementsRDD.groupByKey();

		// Select the values. The key is useless for the next steps
		// Each value is one window composed on 3 elements (time stamp,
		// temperature)
		JavaRDD<Iterable<TimeStampTemperature>> windowsRDD = timestampsWindowsRDD.values();

		// Filter the windows based on their content
		JavaRDD<Iterable<TimeStampTemperature>> seletedWindowsRDD = windowsRDD
				.filter((Iterable<TimeStampTemperature> listElements) -> {

					// Store the 3 elements of the window in a HashMap
					// containing pairs (time stamp, temperature).
					HashMap<Integer, Double> timestampTemp = new HashMap<Integer, Double>();

					// Compute also the information about minimum time stamp
					int minTimestamp = Integer.MAX_VALUE;

					for (TimeStampTemperature element : listElements) {
						timestampTemp.put(element.getTimestamp(), element.getTemperature());

						if (element.getTimestamp() < minTimestamp) {
							minTimestamp = element.getTimestamp();
						}
					}

					// Check if the list contains three elements.
					// If the number of elements is not equal to 3 the window is
					// incomplete and must be discarded
					boolean increasing;

					if (timestampTemp.size() == 3) {
						// Check is the increasing trend is satisfied
						increasing = true;

						for (int ts = minTimestamp + 60; ts <= minTimestamp + 120 && increasing == true; ts = ts + 60) {

							// Check if temperature(t)>temperature(t-60s)
							if (timestampTemp.get(ts) <= timestampTemp.get(ts - 60)) {
								increasing = false;
							}
						}
					} else {
						increasing = false;
					}

					return increasing;
				});

		// seletedWindowsRDD
		seletedWindowsRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
