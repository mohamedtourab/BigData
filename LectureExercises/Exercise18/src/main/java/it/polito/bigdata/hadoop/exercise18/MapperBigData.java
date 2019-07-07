package it.polito.bigdata.hadoop.exercise18;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper of a map-only job
 */
class MapperBigData extends Mapper<LongWritable, // Input key type
		Text, // Input value type
		Text, // Output key type
		NullWritable> {// Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		String record = value.toString();
		// Split each record by using the field separator
		// fields[0]= sensor id
		// fields[1]= date
		// fields[2]= hour:minute
		// fields[3]= temperature
		String[] fields = record.split(",");

		float temperature = Float.parseFloat(fields[3]);

		if (temperature > 30.0)
			context.write(value, NullWritable.get());

	}

}
