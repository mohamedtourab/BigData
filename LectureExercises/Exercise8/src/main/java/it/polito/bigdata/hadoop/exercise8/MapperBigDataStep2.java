
package it.polito.bigdata.hadoop.exercise8;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 8 - Mapper 2
 */
class MapperBigDataStep2 extends 
	Mapper<Text, // input key type
		Text, // input value type
		Text, // output key type
		DoubleWritable> {// output value type 
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{

		String[] date = key.toString().split("-");
		String year = new String(date[0]);
		context.write(new Text(year), new DoubleWritable(Double.parseDouble(value.toString())));

	}
}
