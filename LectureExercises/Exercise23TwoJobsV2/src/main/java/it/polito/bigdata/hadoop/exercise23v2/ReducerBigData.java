package it.polito.bigdata.hadoop.exercise23v2;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 22 - Reducer
 */
class ReducerBigData extends Reducer<NullWritable, // Input key type
		Text, // Input value type
		Text, // Output key type
		NullWritable> { // Output value type

	@Override
	protected void reduce(NullWritable key, // Input key type
			Iterable<Text> values, // Input value type
			Context context) throws IOException, InterruptedException {

		// Iterate over the set of values and emit one line for each of friend of the
		// user of interest
		for (Text value : values) {
			context.write(new Text(value.toString()), NullWritable.get());
		}
	}
}
