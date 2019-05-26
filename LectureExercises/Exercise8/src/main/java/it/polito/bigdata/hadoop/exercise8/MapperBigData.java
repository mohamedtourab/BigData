package it.polito.bigdata.hadoop.exercise8;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 8 - Mapper
 */
class MapperBigData extends
		Mapper<	Text,// input key
				Text,// input value
				Text,//	output key
				DoubleWritable>{//output value
	
	public void map(Text key,Text value,Context context)throws IOException,InterruptedException {
		
		String [] date = key.toString().split("-");
		 context.write(new Text(date[0]+"-"+date[1]), new DoubleWritable(Double.parseDouble(value.toString())));
		
	}
}