package it.polito.bigdata.hadoop.exercise8;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 8 - Reducer
 */

class ReducerBigData extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	@Override
	public void reduce(Text key,Iterable<DoubleWritable> values, Context context) throws IOException,InterruptedException {
		
		double totalIncome = 0;

		for(DoubleWritable value : values) {
			totalIncome+=value.get();
		}
		context.write(new Text(key), new DoubleWritable(totalIncome));
		
		
	}
	
}