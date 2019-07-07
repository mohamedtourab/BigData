package it.polito.bigdata.spark.exercise38;

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
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #38");

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
		// Each pair contains a sensorId (key) and a +1 (value)
		// It can be implemented by using the mapToPair transformation
		JavaPairRDD<String, Integer> sensorsPM10CriticalValuesRDD = readingsHighValueRDD.mapToPair(
				PM10Reading -> {

			String sensorID;
			Tuple2<String, Integer> pair;

			// Split the line in fields
			String[] fields = PM10Reading.split(",");

			// fields[0] contains the sensorId
			sensorID = fields[0];

			pair = new Tuple2<String, Integer>(sensorID, 1);

			return pair;
		});

		// Count the number of critical values for each sensor
		// by using the reduceByKey transformation
		JavaPairRDD<String, Integer> sensorsCountsRDD = sensorsPM10CriticalValuesRDD
				.reduceByKey((intermediateElement1, intermediateElement2) -> 
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
