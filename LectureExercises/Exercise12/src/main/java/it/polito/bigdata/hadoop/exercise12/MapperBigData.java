package it.polito.bigdata.hadoop.exercise12;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper
 */
class MapperBigData extends Mapper<Text, Text, Text, FloatWritable>{
	Float threshold;
	
	protected void setup(Context context) {
		threshold = new Float(context.getConfiguration().get("maxThreshold")); 
	}
	
	public void map(Text key,Text value,Context context)throws IOException,InterruptedException {
		float readingValue = Float.parseFloat(value.toString());
		
		if (readingValue<threshold) {
			
			context.write(new Text(key), new FloatWritable(readingValue));
			
		}
		
	}
}