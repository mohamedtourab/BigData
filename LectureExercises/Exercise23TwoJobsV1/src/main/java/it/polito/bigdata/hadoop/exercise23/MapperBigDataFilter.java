package it.polito.bigdata.hadoop.exercise23;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 23 - Mapper
 */
class MapperBigDataFilter extends Mapper<LongWritable, // Input key type
		Text, // Input value type
		NullWritable, // Output key type
		Text> {// Output value type

	// Identity mapper
	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// It simply reads the input data and emit a copy of it to the single invocation
		// of the reducer that is used to compute the global final result
		// Each emitted pair contains a local subset of the potential friends of the
		// user of interest
		context.write(NullWritable.get(), new Text(value.toString()));
	}

}
