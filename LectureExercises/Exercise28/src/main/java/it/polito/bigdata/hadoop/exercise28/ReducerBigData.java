package it.polito.bigdata.hadoop.exercise28;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * WordCount Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                Text,  // Input value type
                NullWritable,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	String record;
    	ArrayList<String> answers=new ArrayList<String>();
    	String question=null;
 
        // Iterate over the set of values and store the answer records in 
    	// answers and the question record in question
        for (Text value : values) {
        	
        	String table_record=value.toString();
        	
        	if (table_record.startsWith("Q:")==true)
        	{	// This is the question record
        		record=table_record.replaceFirst("Q:", "");
        		question=record;
        	}
        	else
        	{	// This is an answer record
        		record=table_record.replaceFirst("A:", "");
        		answers.add(record);
        	}
        }

        // Emit one pair (question, answer) for each answer
        for (String answer:answers)
        {
        	context.write(NullWritable.get(), new Text(question+","+answer));
        }
    }
}
