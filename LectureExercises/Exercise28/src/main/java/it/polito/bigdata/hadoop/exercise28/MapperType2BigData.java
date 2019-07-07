package it.polito.bigdata.hadoop.exercise28;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper second data format
 */
class MapperType2BigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    		
    		// Record format
			// AnswerId,QuestionId,Timestamp,TextOfTheAnswer
			String[] fields=value.toString().split(",");
		
			String answerId=fields[0];
			String answerText=fields[3];
			String questionId=fields[1];
		
			// Key = questionId
			// Value = A:+answerId,answerText
			// A: is used to specify that this pair has been emitted by
			// analyzing an answer
			context.write(new Text(questionId), new Text("A:"+answerId+","+answerText));
    }
    

    
}
