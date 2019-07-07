package it.polito.bigdata.hadoop.exercise25;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 25 - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, 		  // Input key type
                    Text, 		  // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    
    protected void map(
            LongWritable key, 	// Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Extract username1 and username2
            String[] users = value.toString().split(",");
            
            // Emit two key-value pairs
            // (username1,username2)
            // (username2,username1)
            context.write(new Text(users[0]), new Text(users[1]));
            context.write(new Text(users[1]), new Text(users[0]));
    }
}
