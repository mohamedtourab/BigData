package it.polito.bigdata.hadoop.exercise29;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * WordCount Reducer
 */
class ReducerBigData extends
		Reducer<Text, // Input key type
				Text, // Input value type
				NullWritable, // Output key type
				Text> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<Text> values, // Input value type
			Context context) throws IOException, InterruptedException {

		int numElements;
		String userData = null;

		// Iterate over the set of values and check if
		// 1) there are three elements (one related do the users table and two
		// related to the like table
		// 2) store the information about the "profile/user" element

		numElements = 0;
		for (Text value : values) {

			String table_record = value.toString();

			numElements++;

			if (table_record.startsWith("U") == true) {
				// This is the user data record
				userData = table_record.replaceFirst("U:", "");
			}
		}

		// Emit a pair (null,user data) if the number of elements is equal to 3
		// (2 likes and 1 user data record)
		if (numElements == 3) {
			context.write(NullWritable.get(), new Text(userData));
		}
	}
}
