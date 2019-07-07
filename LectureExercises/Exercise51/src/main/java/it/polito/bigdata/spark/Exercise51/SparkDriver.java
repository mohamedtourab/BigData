package it.polito.bigdata.spark.Exercise51;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) throws InterruptedException {

		String outputPathPrefix;

		outputPathPrefix = args[0];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf()
				.setAppName("Spark Streaming - Exercise #51");

		// Create a Spark Streaming Context object
		// Batch duration = 30 seconds
		// JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(30));
		JavaStreamingContext jssc = 
				new JavaStreamingContext(conf, Durations.seconds(30));

		// Create a (Receiver) DStream that will connect to localhost:9999
		JavaReceiverInputDStream<String> prices = 
				jssc.socketTextStream("localhost", 9999);

		// Computer for each stockID the price variation (compute it for each batch).
		// Select only the stocks wit ha price variation (%) greater than 0.5%

		// Return one pair (stockId,price) for each input record
		JavaPairDStream<String, MaxMin> stockIdPrice = prices
				.mapToPair(record -> {
			String[] fields = record.split(",");

			String stockId = fields[1];
			double price = Double.parseDouble(fields[2]);

			return new 
			Tuple2<String, MaxMin>(stockId, new MaxMin(price, price));
		});

		// Compute max and min for each stockId
		JavaPairDStream<String, MaxMin> stockIdMaxMin = stockIdPrice
			.reduceByKey((MaxMin v1, MaxMin v2) -> {

			double max;
			double min;

			if (v1.getMaxPrice() > v2.getMaxPrice())
				max = v1.getMaxPrice();
			else
				max = v2.getMaxPrice();

			if (v1.getMinPrice() < v2.getMinPrice())
				min = v1.getMinPrice();
			else
				min = v2.getMinPrice();

			return new MaxMin(max, min);
		});

		// Select only the stocks with variation greater than 0.5%
		JavaPairDStream<String, MaxMin> selectedStockIdsVariations = stockIdMaxMin
				.filter((Tuple2<String, MaxMin> stockVariation) -> 
				{
					if (stockVariation._2()
							.computePercentageVariation() > 0.5)
						return true;
					else
						return false;
				});

		selectedStockIdsVariations.print();

		selectedStockIdsVariations.dstream()
		.saveAsTextFiles(outputPathPrefix, "");

		// Start the computation
		jssc.start();

		jssc.awaitTerminationOrTimeout(120000);

		jssc.close();

	}
}
