package it.polito.bigdata.hadoop.lab3;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab3 - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                IntWritable,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
	private TopKVector<WordCountWritable> localTop100;
    protected void setup(Context context) {
		localTop100 = new TopKVector<WordCountWritable>(100);
	}
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	
		int occurrences = 0;
		
		for(IntWritable value:values) {
			occurrences+=value.get();
		}
		WordCountWritable newEntry = 
    			new WordCountWritable(key.toString(),new Integer(occurrences));
  
		
		localTop100.updateWithNewElement(newEntry);
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	
    	Vector<WordCountWritable> top100Objects = localTop100.getLocalTopK();
    	for(WordCountWritable value: top100Objects) {
    		context.write(new Text(value.getWord()), new IntWritable(value.getCount()));
    	}
    	
    	
    	
	}
}
