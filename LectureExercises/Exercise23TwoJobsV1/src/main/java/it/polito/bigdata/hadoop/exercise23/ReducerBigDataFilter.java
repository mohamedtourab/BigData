package it.polito.bigdata.hadoop.exercise23;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;

/**
 * Exercise 23 - Reducer
 */
class ReducerBigDataFilter extends Reducer<NullWritable, // Input key type
		Text, // Input value type
		Text, // Output key type
		NullWritable> { // Output value type

	@Override
	protected void reduce(NullWritable key, // Input key type
			Iterable<Text> values, // Input value type
			Context context) throws IOException, InterruptedException {

		HashSet<String> potentialFriends = new HashSet<String>();

		for (Text value : values) {
			// Each value is one potential friend
			potentialFriends.add(value.toString());
		}

		String globalPotFriends;

		// Concatenate the users in potentialFriends
		globalPotFriends = new String("");

		for (String potFriend : potentialFriends) {
			globalPotFriends = globalPotFriends.concat(potFriend + " ");
		}

		context.write(new Text(globalPotFriends), NullWritable.get());
	}
	
}
