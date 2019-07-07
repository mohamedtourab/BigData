package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                FloatWritable,    // Input value type
                Text,           // Output key type
                FloatWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<FloatWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
    	int numberOfReviews =0;
    	float sum = 0;
    	float average=0;
    	for(FloatWritable value: values) {
    		sum+=Float.parseFloat(value.toString());
    		numberOfReviews++;
    	}
    	average = (float)sum/numberOfReviews;
    	context.write(new Text(key), new FloatWritable(average));
    }
}
