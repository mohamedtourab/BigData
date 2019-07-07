package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                FloatWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
    	int numberOfReviews =0;
    	int sum = 0;
    	float average=0;
    	String[] fields = null;
    	for(Text value: values) {
    		fields = value.toString().split("_");
    		sum+=Integer.parseInt(fields[1]);
    		numberOfReviews++;
    	}
    	average = (float)sum/numberOfReviews;
    	context.write(new Text(fields[0]), new FloatWritable(average));
    	
    }
}
