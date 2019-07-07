package it.polito.bigdata.hadoop.exercise23;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;

/**
 * Exercise 23 - Reducer
 */
class ReducerBigData extends Reducer<Text, // Input key type
		Text, // Input value type
		Text, // Output key type
		NullWritable> { // Output value type

	HashSet<String> finalListPotentialFriends;
	String specifiedUser;

	protected void setup(Context context) throws IOException, InterruptedException {
		// Retrieve the information about the user of interest
		specifiedUser = context.getConfiguration().get("username");

		// Instantiate the local variable that is used to store the complete
		// list of potential friends
		finalListPotentialFriends = new HashSet<String>();
	}

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<Text> values, // Input value type
			Context context) throws IOException, InterruptedException {

		boolean containsSpecifiedUser;

		// Partial list of potential friends
		HashSet<String> partialListOfPotentialFriends = new HashSet<String>();

		// Key contains one username.
		// If values contains the specified user it means that the specified
		// user and the other users in values have user "key" in common.
		// Hence, the users in values are potential friends
		containsSpecifiedUser = false;

		for (Text value : values) {
			if (specifiedUser.compareTo(value.toString()) == 0)
				containsSpecifiedUser = true;
			else {
				// Store the list of users for a potential "second iteration"
				partialListOfPotentialFriends.add(value.toString());
			}
		}

		// If containsSpecifiedUser is true it means that
		// partialListOfPotentialFriends
		// contains potential friends of the specified user
		// It is useful if and only if partialListOfPotentialFriends is not
		// empty (i.e., if values
		// contains the selected user and also another one)

		if (containsSpecifiedUser == true && partialListOfPotentialFriends.size() > 0) {
			// Extract the list of potential users for
			// partialListOfPotentialFriends

			for (String user : partialListOfPotentialFriends) {
				// If the user is new then it is inserted in the set
				// Otherwise, if it is already in the set, it is ignored
				finalListPotentialFriends.add(user);
			}
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		// Emit the partial list of potential friends of the user of interest.
		// In this first job I do not need to emit one single line.
		// Emit one line for each potential friend

		for (String potFriend : finalListPotentialFriends) {
			context.write(new Text(potFriend), NullWritable.get());
		}
	}

}
