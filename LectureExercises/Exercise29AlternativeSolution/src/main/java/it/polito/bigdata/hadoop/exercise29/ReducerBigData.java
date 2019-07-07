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

		boolean commedia;
		boolean adventure;
		String userData = null;

		// Iterate over the set of values and check if
		// both commedia and adventure are present in the list of
		// genres liked by the current user (key=userId)
		commedia = false;
		adventure = false;

		for (Text value : values) {

			String table_record = value.toString();

			if (table_record.startsWith("L:") == true) { // This is a like
															// record
				if (table_record.compareTo("L:Commedia") == 0)
					commedia = true;
				else if (table_record.compareTo("L:Adventure") == 0)
					adventure = true;
			} else {
				// This is the user data record
				userData = table_record.replaceFirst("U:", "");
			}
		}

		// Emit a pair (null,user data) if the user likes both
		// Commedia and Adventure movies
		if (commedia == true && adventure == true) {
			context.write(NullWritable.get(), new Text(userData));
		}
	}
}
