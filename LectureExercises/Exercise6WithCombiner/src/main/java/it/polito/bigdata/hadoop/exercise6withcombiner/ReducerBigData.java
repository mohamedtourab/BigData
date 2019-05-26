package it.polito.bigdata.hadoop.exercise6withcombiner;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * WordCount Reducer
 */
class ReducerBigData extends
		Reducer<Text, // Input key type
				Text,// Input value type
				Text, // Output key type
				Text> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<Text> values, // Input value type
			Context context) throws IOException, InterruptedException {
		double min = Double.MAX_VALUE;
		double max = Double.MIN_VALUE;

		// Iterate over the set of values and update max and min.
		// The format of each input value is max_min
		for (Text value : values) {
			// fields[0] = max
			// fields[1] = min
			String[] fields = value.toString().split("_");

			if (Double.parseDouble(fields[0]) > max) {
				max = Double.parseDouble(fields[0]);
			}

			if (Double.parseDouble(fields[1]) < min) {
				min = Double.parseDouble(fields[1]);
			}
		}// Compute average value
		// Emits pair (sensor_id, average)
		
		context.write(new Text(key), new Text("max=" + max + "_min=" + min));
	}
}














/*
 * package it.polito.bigdata.hadoop.exercise5;
 * 
 * import java.io.IOException;
 * 
 * import org.apache.hadoop.io.*; import org.apache.hadoop.mapreduce.Reducer;
 * 
 *//**
	 * Exercise 2 - Reducer
	 *//*
		 * class ReducerBigData extends Reducer<Text, // Input key type FloatWritable,
		 * // Input value type Text, // Output key type FloatWritable> { // Output value
		 * type
		 * 
		 * @Override protected void reduce(Text key, // Input key type
		 * Iterable<FloatWritable> values, // Input value type Context context) throws
		 * IOException, InterruptedException {
		 * 
		 * double sum = 0; int counter = 0;
		 * 
		 * // Iterate over the set of values and sum them for (FloatWritable value :
		 * values) { sum = sum + value.get(); counter++; }
		 * 
		 * context.write(new Text(key), new FloatWritable((float) sum / counter)); } }
		 */