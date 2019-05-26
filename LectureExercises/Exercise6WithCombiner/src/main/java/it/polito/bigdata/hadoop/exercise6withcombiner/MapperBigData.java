package it.polito.bigdata.hadoop.exercise6withcombiner;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Average Mapper
 */
class MapperBigData extends
		Mapper<LongWritable, // Input key type
				Text, // Input value type
				Text, // Output key type
				Text> {// Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Split each record by using the field separator
		// fields[0]= first attribute - sensor id
		// fields[1]= second attribute - date
		// fields[2]= third attribute - PM10 value
		String[] fields = value.toString().split(",");
		String sensorId = fields[0];
		float PM10value = Float.parseFloat(fields[2]);

		// emit the pair (sensor_id, reading value)
		context.write(new Text(sensorId), new Text(PM10value+"_"+PM10value));
	}
}





















/*
 * package it.polito.bigdata.hadoop.exercise5;
 * 
 * import java.io.IOException;
 * 
 * import org.apache.hadoop.io.*; import org.apache.hadoop.mapreduce.Mapper;
 * 
 * 
 *//**
	 * Exercise 2 - Mapper
	 *//*
		 * class MapperBigData extends Mapper<LongWritable, // Input key type Text, //
		 * Input value type Text, // Output key type FloatWritable> {// Output value
		 * type
		 * 
		 * protected void map(LongWritable key, // Input key type Text value, // Input
		 * value type Context context) throws IOException, InterruptedException {
		 * 
		 * // Split each sentence in words. Use whitespace(s) as delimiter // (=a space,
		 * a tab, a line break, or a form feed) // The split method returns an array of
		 * strings String[] words = key.toString().split(","); String sensorId =
		 * words[0]; Float readingValue = Float.parseFloat(words[2].toString());
		 * 
		 * 
		 * // emit the pair (word, 1) context.write(new Text(sensorId), new
		 * FloatWritable(new Float(readingValue)));
		 * 
		 * } }
		 */