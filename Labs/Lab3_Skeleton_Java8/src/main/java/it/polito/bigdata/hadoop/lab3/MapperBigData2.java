package it.polito.bigdata.hadoop.lab3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    WordCountWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		 WordCountWritable record = new WordCountWritable(value.toString().split("\\s+")[0],new Integer(Integer.parseInt(value.toString().split("\\s+")[1])));
    		
    		 context.write(NullWritable.get() , new WordCountWritable(record));
    		 
    }
}
