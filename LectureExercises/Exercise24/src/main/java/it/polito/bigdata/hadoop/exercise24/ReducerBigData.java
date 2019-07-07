package it.polito.bigdata.hadoop.exercise24;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 24 - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, 		// Input key type
        Iterable<Text> values, 	// Input value type
        Context context) throws IOException, InterruptedException {

    	String listOfFriends=new String("");
    	
        // Key contains one userame.
    	// Iterate over the set of values and concatenate them to build the 
    	// list of friend of the username specified in key.
        for (Text value : values) {
        	listOfFriends=listOfFriends.concat(value.toString()+" ");
        }
        
        context.write(new Text(key+":"), new Text(listOfFriends));
    }
}
