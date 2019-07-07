package it.polito.bigdata.spark.exercise37;

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
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #37");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the input file
		JavaRDD<String> readingsRDD = sc.textFile(inputPath);

		// Create a PairRDD
		// Each pair contains a sensorId (key) and a PM10 value (value)
		// It can be implemented by using the mapToPair transformation
		JavaPairRDD<String, Double> sensorsPM10ValuesRDD = readingsRDD.mapToPair(PM10Reading -> {

			Double PM10value;
			String sensorID;
			Tuple2<String, Double> pair;

			// Split the line in fields
			String[] fields = PM10Reading.split(",");

			// fields[0] contains the sensorId
			sensorID = fields[0];

			// fields[2] contains the PM10 value
			PM10value = new Double(fields[2]);

			pair = new Tuple2<String, Double>(sensorID, PM10value);

			return pair;
		});

		// Apply the reduceByKey transformation to compute the maximum PM10
		// value for each sensor

		JavaPairRDD<String, Double> sensorsMaxValuesRDD = sensorsPM10ValuesRDD.reduceByKey((value1, value2) -> {
			if (value1.compareTo(value2) > 0)
				return value1;
			else
				return value2;
		});

		sensorsMaxValuesRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
