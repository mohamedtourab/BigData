package it.polito.bigdata.hadoop.exercise9;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 9 - Reducer
 */
class ReducerBigData extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
		int occurrences =0;
		
		for(IntWritable value: values) {
			occurrences += value.get();
		}
		context.write(new Text(key), new IntWritable(occurrences));
		
	}
	
}