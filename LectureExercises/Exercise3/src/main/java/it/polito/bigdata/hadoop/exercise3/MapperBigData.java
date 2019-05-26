package it.polito.bigdata.hadoop.exercise3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 2 - Mapper
 */
class MapperBigData extends Mapper<Text, // Input key type
		Text, // Input value type
		Text, // Output key type
		IntWritable> {// Output value type

	private static Double Threshold = new Double(50);

	protected void map(Text key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Split each sentence in words. Use whitespace(s) as delimiter
		// (=a space, a tab, a line break, or a form feed)
		// The split method returns an array of strings
		String[] words = key.toString().split(",");
		String sensorId = words[0];

		Double pm10Value = new Double(value.toString());

		if (pm10Value > Threshold) {
			// emit the pair (word, 1)
			context.write(new Text(sensorId), new IntWritable(1));
		}

	}
}
