package it.polito.bigdata.spark.exercise38;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.util.ArrayList;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #38");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the input file
		JavaRDD<String> readingsRDD = sc.textFile(inputPath);

		// Apply a flatMapToPair transformation to emit (sensorId, 1) only for
		// the lines with PM10>50
		JavaPairRDD<String, Integer> sensorsPM10CriticalValuesRDD = readingsRDD.flatMapToPair(
				PM10Reading -> {

			// Split the line in fields
			String[] fields = PM10Reading.split(",");

			// fields[0] contains the sensorId
			String sensorID = fields[0];

			// fields[2] contains the PM10 value
			double PM10value = Double.parseDouble(fields[2]);
			ArrayList<Tuple2<String, Integer>> sensorOne = new ArrayList<Tuple2<String, Integer>>();

			if (PM10value > 50) {

				Tuple2<String, Integer> localPair = new Tuple2<String, Integer>(sensorID, 1);
				sensorOne.add(localPair);
			}

			return sensorOne.iterator();

		});


		// Count the number of critical values for each sensor
		// by using the reduceByKey transformation
		JavaPairRDD<String, Integer> sensorsCountsRDD = sensorsPM10CriticalValuesRDD
				.reduceByKey((Integer intermediateElement1, Integer intermediateElement2) -> 
				new Integer(intermediateElement1 + intermediateElement2));

		// Select only the pairs with a value (number of critical PM10 values)
		// at least equal to 2
		// This is a filter transformation on a PairRDD
		JavaPairRDD<String, Integer> sensorsCountsCriticalRDD = sensorsCountsRDD
				.filter((Tuple2<String, Integer> sensorCountPair) -> {
					// The second field of the tuple contains the number of
					// critical PM10 values
					// If the number of critical values is >= 2 returns true
					if (sensorCountPair._2().intValue() >= 2)
						return true;
					else
						return false;
				});

		sensorsCountsCriticalRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
