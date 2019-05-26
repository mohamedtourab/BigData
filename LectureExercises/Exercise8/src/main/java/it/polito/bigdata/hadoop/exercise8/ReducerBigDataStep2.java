package it.polito.bigdata.hadoop.exercise8;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 8 - Reducer 2
 */

class ReducerBigDataStep2 extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	@Override
	public void reduce(Text key,Iterable<DoubleWritable> values, Context context) throws InterruptedException,IOException {
		double totalYearIncome = 0;
		int counter = 0;
		
		for(DoubleWritable value : values)
		{
			totalYearIncome+=value.get();
			counter = counter+1;
		}
		context.write(new Text(key), new DoubleWritable(totalYearIncome/counter));
	}
	
}