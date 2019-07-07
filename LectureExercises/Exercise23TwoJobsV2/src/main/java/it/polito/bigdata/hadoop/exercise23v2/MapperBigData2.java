package it.polito.bigdata.hadoop.exercise23v2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 23 - Mapper Job 2
 */
class MapperBigData2 extends Mapper<LongWritable, // Input key type
		Text, // Input value type
		NullWritable, // Output key type
		Text> {// Output value type

	String specifiedUser;

	ArrayList<String> friends;

	protected void setup(Context context) throws IOException, InterruptedException {
		String line;

		// Store the information about the user of interest
		specifiedUser = context.getConfiguration().get("username");

		// Store in the ArraList friends the list of friends available in the
		// shared file
		friends = new ArrayList<String>();

		URI[] CachedFiles = context.getCacheFiles();

		// This application has one single single cached file.
		// Its path is CachedFiles[0].getPath()
		BufferedReader fileFriends = new BufferedReader(new FileReader(new File(CachedFiles[0].getPath())));

		// There is one friend per line
		while ((line = fileFriends.readLine()) != null) {
			friends.add(line);
		}

		fileFriends.close();
	}

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Extract username1 and username2
		String[] users = value.toString().split(",");

		// Check if one of the two users is friend of the user of interest.
		// If it is true, the the other user of the current pair is a potential friend
		// of the user of interest

		if (friends.contains(users[0]) == true && users[1].compareTo(specifiedUser) != 0) {
			// users[0] is a friend of specifiedUser
			// users[1] is a potential friend of specifiedUser
			// emit the pair (null, users[1])
			context.write(NullWritable.get(), new Text(users[1]));
		}

		if (friends.contains(users[1]) == true && users[0].compareTo(specifiedUser) != 0) {
			// users[1] is a friend of specifiedUser
			// users[0] is a potential friend of specifiedUser
			// emit the pair (null, users[0])
			context.write(NullWritable.get(), new Text(users[0]));
		}

	}
}
