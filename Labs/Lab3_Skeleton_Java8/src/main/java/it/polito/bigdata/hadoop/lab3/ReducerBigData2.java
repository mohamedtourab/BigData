package it.polito.bigdata.hadoop.lab3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                NullWritable,           // Input key type
                WordCountWritable,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
	
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<WordCountWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	TopKVector<WordCountWritable> GlobalTop100=new TopKVector<WordCountWritable>(100);
    	
    	for(WordCountWritable value:values) {
    		GlobalTop100.updateWithNewElement(new WordCountWritable(value));
    	}
    	
    	for (WordCountWritable wordCountWritable : GlobalTop100.getLocalTopK()) {
			context.write(new Text(wordCountWritable.getWord()), new IntWritable(wordCountWritable.getCount()));
		}
		
    	
    }
}
