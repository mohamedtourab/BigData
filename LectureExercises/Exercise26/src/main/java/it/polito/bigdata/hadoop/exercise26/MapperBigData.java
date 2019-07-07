package it.polito.bigdata.hadoop.exercise26;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Exercise 26 - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type
    
	private HashMap<String,Integer> dictionary;
	
	@SuppressWarnings("deprecation")
	protected void setup(Context context) throws IOException, InterruptedException
	{
		String line;
		String word;
		Integer intValue;

		
		dictionary=new HashMap<String, Integer>();
		// Open the dictionary file (that is shared by means of the distributed 
		// cache mechanism) 
		
		
		Path[] PathsCachedFiles = context.getLocalCacheFiles();	
		
		// This application has one single single cached file. 
		// Its path is URIsCachedFiles[0] 
		BufferedReader fileStopWords = new BufferedReader
				(new FileReader(
						new File(PathsCachedFiles[0].toString())));
	
		
		// Each line of the file contains one mapping 
		// word integer 
		// The mapping is stored in the dictionary HashMap variable
		while ((line = fileStopWords.readLine()) != null) {
			
			// record[0] = integer value associated with the word
			// record[1] = word
			String[] record=line.split("\t");
			intValue=new Integer(record[0]);
			word=record[1];
			
			dictionary.put(word,intValue);
		}
	
		fileStopWords.close();
	}

	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		String convertedString;

    		Integer intValue;

            // Split each sentence in words. Use whitespace(s) as delimiter (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
            String[] words = value.toString().split("\\s+");

            
            // Convert words to integers
            convertedString=new String("");
            
            // Iterate over the set of words
            for(String word : words) {
            
            	// Retrieve the integer associated with the current word
            	intValue=dictionary.get(word.toUpperCase());
            	
            	convertedString=convertedString.concat(intValue+" ");
            }
            
            // emit the pair (null, sentenceWithoutStopwords)
            context.write(NullWritable.get(), new Text(convertedString));
    }
}
