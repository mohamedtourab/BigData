package it.polito.bigdata.hadoop.exercise4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 2 - Reducer
 */
class ReducerBigData extends Reducer<Text, // Input key type
		Text, // Input value type
		Text, // Output key type
		Text> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<Text> values, // Input value type
			Context context) throws IOException, InterruptedException {

		String date = new String();

		// Iterate over the set of values and sum them
		for (Text text : values) {
			if (date.length() == 0) {
				date = date.concat(text.toString());
			} else {
				date = date.concat(","+text.toString());
			}

		}

		context.write(key, new Text(date));
	}
}
