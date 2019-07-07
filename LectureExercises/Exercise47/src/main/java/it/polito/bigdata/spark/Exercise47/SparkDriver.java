package it.polito.bigdata.spark.Exercise47;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver {

	@SuppressWarnings("resource")
	public static void main(String[] args) throws InterruptedException {

		String inputFile;
		String outputPathPrefix;

		inputFile = args[0];
		outputPathPrefix = args[1];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Streaming #47");

		// Create a Spark Streaming Context object
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));

		JavaSparkContext sc = jssc.sparkContext();

		// "Standard" RDD associated with the characteristics of the stations
		JavaPairRDD<String, String> stationName = sc.textFile(inputFile).mapToPair(line -> {
			// id latitude longitude name
			// 1 41.397978 2.180019 Gran Via Corts Catalanes

			String[] fields = line.split("\\t");

			return new Tuple2<String, String>(fields[0], fields[3]);
		});


		// Create a (Receiver) DStream that will connect to localhost:9999
		JavaReceiverInputDStream<String> readings = jssc.socketTextStream("localhost", 9999);

		// Input stream
		// Each readings has the format:
		// stationId,#free slots,#used slots,timestamp
		// Select readings with num. free slots = 0
		JavaDStream<String> fullReadings = readings.filter(reading -> {
			String[] fields = reading.split(",");

			if (Integer.parseInt(fields[1]) == 0) {
				return true;
			} else {
				return false;
			}
		});

		// Create a PairDStream containing pairs <StationId, timestamp>
		JavaPairDStream<String, String> stationIdTime = fullReadings.mapToPair(reading -> {
			String[] fields = reading.split(",");
			return new Tuple2<String, String>(fields[0], fields[3]);
		});

		// Join the content of the DStream with the "standard" RDD to retrieve
		// the
		// name of the station. To perform this join bewteen straming and
		// non-streaming RDD
		// the transform transformation must be used
		// <name of the station, timestamp>
		final JavaDStream<Tuple2<String, String>> stationNameTime = stationIdTime
				.transform((JavaPairRDD<String, String> contentStreamRdd) -> {
					// Join the RDD associated with the current batch of the
					// stream with the "standard" RDD stationName
					JavaPairRDD<String, Tuple2<String, String>> stationIDTimestampName = contentStreamRdd
							.join(stationName);

					// Return the pair (station name, timestamp) 
					return stationIDTimestampName.values();

				});

		stationNameTime.print();

		stationNameTime.dstream().saveAsTextFiles(outputPathPrefix, "");

		// Start the computation
		jssc.start();

		jssc.awaitTerminationOrTimeout(120000);

		jssc.close();

	}
}
