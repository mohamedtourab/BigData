package it.polito.bigdata.hadoop.exercise23;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 23 - Mapper
 */
class MapperBigData extends Mapper<LongWritable, // Input key type
		Text, // Input value type
		Text, // Output key type
		Text> {// Output value type

	String specifiedUser;

	protected void setup(Context context) throws IOException, InterruptedException {
		// Retrieve the information about the user of interest
		specifiedUser = context.getConfiguration().get("username");
	}

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Extract username1 and username2
		String[] users = value.toString().split(",");

		// Emit two key-value pairs
		// (username1,username2)
		// (username2,username1)
		// Do not emit pair with key=user of interest. It is not useful
		if (specifiedUser.compareTo(users[0]) != 0)
			context.write(new Text(users[0]), new Text(users[1]));

		if (specifiedUser.compareTo(users[1]) != 0)
			context.write(new Text(users[1]), new Text(users[0]));
	}
}
