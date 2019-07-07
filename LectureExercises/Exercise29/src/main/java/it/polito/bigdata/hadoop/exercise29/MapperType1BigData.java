package it.polito.bigdata.hadoop.exercise29;

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
    		// UserId,Name,Surname,Gender,YearOfBirth,City,Education
    		String[] fields=value.toString().split(",");
			
			String userId=fields[0];
			String gender=fields[3];
			String yearOfBirth=fields[4];
			
			// Key = userId
			// Value = U:+gender,yearOfBirth
			// U: is used to specify that this pair has been emitted by
			// analyzing the user file
            context.write(new Text(userId), new Text("U:"+gender+","+yearOfBirth));
    }
    

    
}
