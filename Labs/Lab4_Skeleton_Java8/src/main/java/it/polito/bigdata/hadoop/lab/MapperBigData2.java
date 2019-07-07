package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
                    Text, // Input key type
                    FloatWritable,         // Input value type
                    Text,         // Output key type
                    FloatWritable> {// Output value type
    
    protected void map(
            Text key,   // Input key type
            FloatWritable value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
    	context.write(new Text(key), value);
    }
}
