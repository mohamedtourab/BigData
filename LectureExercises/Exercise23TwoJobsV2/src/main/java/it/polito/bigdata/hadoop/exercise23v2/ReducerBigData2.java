package it.polito.bigdata.hadoop.exercise23v2;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 22 - Reducer Job 2
 */
class ReducerBigData2 extends Reducer<NullWritable, // Input key type
		Text, // Input value type
		Text, // Output key type
		NullWritable> { // Output value type

	@Override
	protected void reduce(NullWritable key, // Input key type
			Iterable<Text> values, // Input value type
			Context context) throws IOException, InterruptedException {

		ArrayList<String> potFriends = new ArrayList<String>();
		String listOfPotFriends = new String("");

		// Iterate over the set of values and include them in the ArrayList of
		// potential friends
		for (Text value : values) {
			if (potFriends.contains(value.toString()) == false)
				potFriends.add(value.toString());
		}

		// Concatenate the list of potential friends
		for (String potFriend : potFriends) {
			listOfPotFriends = listOfPotFriends.concat(potFriend + " ");
		}

		// Emit the list of potential friends (in one single line)
		context.write(new Text(listOfPotFriends), NullWritable.get());
	}
}
