package it.polito.bigdata.hadoop.exercise22;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 22 - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, 		  // Input key type
                    Text, 		  // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type
    
    protected void map(
            LongWritable key, 	// Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		String specifiedUser;
            
            specifiedUser=context.getConfiguration().get("username");

            // Extract username1 and username2
            String[] users = value.toString().split(",");
            
            // Check if one of the users is specifiedUser
            if (specifiedUser.compareTo(users[0])==0)
            {
                // emit the pair (null, users[1])
                context.write(NullWritable.get(), new Text(users[1]));
            }

            if (specifiedUser.compareTo(users[1])==0)
            {
                // emit the pair (null, users[0])
                context.write(NullWritable.get(), new Text(users[0]));
            }
            
    }
}
