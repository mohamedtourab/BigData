package it.polito.bigdata.spark.Exercise52;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver {

	public static void main(String[] args) throws InterruptedException {

		String outputPathPrefix;
		String historicalInputFile;

		historicalInputFile = args[0];
		outputPathPrefix = args[1];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Streaming - Exercise #51");

		// Create a Spark Streaming Context object
		// Batch duration = 60 seconds
//		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(60));
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

		// Create a Spark context object
		JavaSparkContext sc = jssc.sparkContext();

		// Read the historical data and compute the maximum and minimum price for each
		// stock
		// Non-streaming RDD
		JavaRDD<String> historicalData = sc.textFile(historicalInputFile);

		JavaPairRDD<String, MaxMin> historicalStockIdPrice = historicalData.mapToPair(record -> {
			String[] fields = record.split(",");

			String stockId = fields[1];
			double price = Double.parseDouble(fields[2]);

			return new Tuple2<String, MaxMin>(stockId, new MaxMin(price, price));
		});

		// Compute max and min for each stockId based on the historical data
		JavaPairRDD<String, MaxMin> historicalStockIdMaxMin = historicalStockIdPrice
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

		// Create a (Receiver) DStream that will connect to localhost:9999
		JavaReceiverInputDStream<String> prices = jssc.socketTextStream("localhost", 9999);

		// Join on the stockid each input record of the input stream with the content of
		// historicalStockIdMaxMin
		// to retrieve the historical maximum-minimum range of the stock

		// Return one pair (stockId,price) for each input record
		JavaPairDStream<String, Double> stockIdPrice = prices.mapToPair(record -> {
			String[] fields = record.split(",");

			String stockId = fields[1];
			double price = Double.parseDouble(fields[2]);

			return new Tuple2<String, Double>(stockId, price);
		});

		// Join
		JavaPairDStream<String, Tuple2<Double, MaxMin>> stockIdPriceMaxMin = stockIdPrice
				.transformToPair((JavaPairRDD<String, Double> contentStreamRdd) -> {
					// Join PairRDD associated with the content of the current batch and the
					// non-streaming RDD historicalStockIdMaxMin
					JavaPairRDD<String, Tuple2<Double, MaxMin>> joinStockIdPriceMaxMin = contentStreamRdd
							.join(historicalStockIdMaxMin);

					// Return the result of the join operation
					return joinStockIdPriceMaxMin;
				});

		// Select only lines with price > maximum or price < minimum
		JavaPairDStream<String, Tuple2<Double, MaxMin>> selectedStockPrices = stockIdPriceMaxMin
				.filter((Tuple2<String, Tuple2<Double, MaxMin>> pair) -> {
					double currentPrice = pair._2()._1();
					MaxMin stockHistoricalMaxMin = pair._2()._2();
					if (currentPrice > stockHistoricalMaxMin.getMaxPrice()
							|| currentPrice < stockHistoricalMaxMin.getMinPrice())
						return true;
					else
						return false;
				});

		// Retrieve only the stockIDs and apply distinct to remove duplicates
		// keys is not available for JavaPairDStream.
		// Transform must be used
		JavaDStream<String> selectStockIds = selectedStockPrices.transform(
				(JavaPairRDD<String, Tuple2<Double, MaxMin>> contentStreamRdd) -> contentStreamRdd.keys().distinct());

		selectStockIds.print();

		selectStockIds.dstream().saveAsTextFiles(outputPathPrefix, "");

		// Start the computation
		jssc.start();

		jssc.awaitTerminationOrTimeout(120000);

		jssc.close();

	}
}
