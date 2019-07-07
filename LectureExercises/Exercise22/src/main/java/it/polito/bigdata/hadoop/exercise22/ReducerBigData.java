package it.polito.bigdata.hadoop.exercise22;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 22 - Reducer
 */
class ReducerBigData extends Reducer<
                NullWritable,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
    
    @Override
    protected void reduce(
        NullWritable key, 		// Input key type
        Iterable<Text> values, 	// Input value type
        Context context) throws IOException, InterruptedException {

    	String listOfFriends=new String("");
    	
        // Iterate over the set of values and concatenate them 
        for (Text value : values) {
        	listOfFriends=listOfFriends.concat(value.toString()+" ");
        }
        
        context.write(new Text(listOfFriends), NullWritable.get());
    }
}
