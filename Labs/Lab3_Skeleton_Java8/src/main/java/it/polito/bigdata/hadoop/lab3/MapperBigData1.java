package it.polito.bigdata.hadoop.lab3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type	
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    	String[] inputProducts = value.toString().split(",");
    	String emittedProduct = new String();
    	
    	//inputProducts.length-2 the 2 because we have at the first position of the array the username
    	for(int i=1;i<inputProducts.length-1;i++) {
    		for (int j = i+1; j < inputProducts.length; j++) {
				if(inputProducts[i].compareTo(inputProducts[j])<0) {
					emittedProduct+= inputProducts[i]+"_"+inputProducts[j];
				}
    			else {
    				emittedProduct+= inputProducts[j]+"_"+inputProducts[i];
				}
				context.write(new Text(emittedProduct), new IntWritable(1));
	    		emittedProduct = "";
			}
    	}
    	
    	
    }
}
