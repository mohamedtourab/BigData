package it.polito.bigdata.hadoop.exercise25;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 25 - Reducer
 */
class ReducerBigDataFilter extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
	
    @Override
    protected void reduce(
        Text key, 		// Input key type
        Iterable<Text> values, 	// Input value type
        Context context) throws IOException, InterruptedException {

    	String listOfPotentialFriends;
    	HashSet<String> potentialFriends;
    	
    	potentialFriends=new HashSet<String>();
    	
        // Iterate over the values and include the users in the final set
    	for (Text user: values)
        {
        	// If the user is new then it is inserted in the set
        	// Otherwise, it is already in the set, it is ignored
        	potentialFriends.add(user.toString());
        }

    	listOfPotentialFriends=new String("");
    	for (String user: potentialFriends)
    	{
    		listOfPotentialFriends=listOfPotentialFriends.concat(user+" ");
    	}

		context.write(new Text(key), new Text(listOfPotentialFriends));
    	
    }
}
