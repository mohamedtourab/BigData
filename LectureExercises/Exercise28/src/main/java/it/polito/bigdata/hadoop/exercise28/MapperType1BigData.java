package it.polito.bigdata.hadoop.exercise28;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper first data format
 */
class MapperType1BigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
	
	

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    	
    		// Record format
    		// QuestionId,Timestamp,TextOfTheQuestion
    		String[] fields=value.toString().split(",");
			
			String questionId=fields[0];
			String questionText=fields[2];
			
			// Key = questionId
			// Value = Q:+questionId,questionText
			// Q: is used to specify that this pair has been emitted by
			// analyzing a question
            context.write(new Text(questionId), new Text("Q:"+questionId+","+questionText));
    }
    

    
}
