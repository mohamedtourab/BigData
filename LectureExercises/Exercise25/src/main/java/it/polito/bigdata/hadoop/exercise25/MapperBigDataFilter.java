package it.polito.bigdata.hadoop.exercise25;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 25 - Mapper
 */
class MapperBigDataFilter extends Mapper<
                    Text, 		  // Input key type
                    Text, 		  // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    
	
    protected void map(
            Text key, 	// Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Emit one key-value pair of each user in value.
    		// Key is equal to the key of the input key-value pair
            String[] users = value.toString().split(" ");
            
            for (String user: users)
            {
        		context.write(new Text(key.toString()), new Text(user));
            }
            
    }

}
