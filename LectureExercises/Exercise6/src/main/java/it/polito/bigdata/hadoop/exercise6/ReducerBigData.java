package it.polito.bigdata.hadoop.exercise6;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * WordCount Reducer
 */
class ReducerBigData extends
		Reducer<Text, // Input key type
				FloatWritable, // Input value type
				Text, // Output key type
				Text> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<FloatWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {

		double max = Double.MIN_VALUE;
		double min = Double.MAX_VALUE;

		// Iterate over the set of values and sum them.
		// Count also the number of values
		for (FloatWritable value : values) {
			if(value.get() > max)
			{
				max = value.get();
			}
			else if(value.get()<min) {
				min = value.get();
			}
			
		}
		String emittedString = new String("max = "+max+"_min = "+min);
		// Compute average value
		// Emits pair (sensor_id, average)
		context.write(new Text(key), new Text(emittedString));
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