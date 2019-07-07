package it.polito.bigdata.hadoop.exercise29;

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
			// UserId,MovieGenre
			String[] fields=value.toString().split(",");
		
			String userId=fields[0];
			String genre=fields[1];
		
			// Key = userId
			// Value = L:+genre
			// L: is used to specify that this pair has been emitted by
			// analyzing the like file
			// Emit the pair if and only if the genre is Commedia or Adventure
			if (genre.compareTo("Commedia")==0 || genre.compareTo("Adventure")==0)
			{
				context.write(new Text(userId), new Text("L:"+genre));
			}
    }
    

    
}
