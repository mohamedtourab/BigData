package it.polito.bigdata.hadoop.exercise21;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Exercise 21 - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type
    
	private ArrayList<String> stopWords;
	
	@SuppressWarnings("deprecation")
	protected void setup(Context context) throws IOException, InterruptedException
	{
		String nextLine;

		
		stopWords=new ArrayList<String>();
		// Open the stopword file (that is shared by means of the distributed 
		// cache mechanism) 
		
		
		Path[] PathsCachedFiles = context.getLocalCacheFiles();	
		
		// This application has one single single cached file. 
		// Its path is URIsCachedFiles[0] 
		BufferedReader fileStopWords = new BufferedReader(new FileReader(new File(PathsCachedFiles[0].toString())));
	
		
		// Each line of the file contains one stopword 
		// The stopwords are stored in the stopWords list
		while ((nextLine = fileStopWords.readLine()) != null) {
			stopWords.add(nextLine);
		}
	
		fileStopWords.close();
	}

	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		boolean stopword;
            // Split each sentence in words. Use whitespace(s) as delimiter (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
            String[] words = value.toString().split("\\s+");

            
            // Remove stopwords from the current sentence
            String sentenceWithoutStopwords=new String("");
            // Iterate over the set of words
            for(String word : words) {
            	
            	// if the current word is in the stopWords list it means it is a 
            	// stopword 
            	if (stopWords.contains(word)==true)
            		stopword=true;
            	else
                	stopword=false;
            		
            	
            	// If the word is a stopword do not consider it
            	// Otherwise attach it at the end of sentenceWithoutStopwords
                if (stopword==false)
                {
                	sentenceWithoutStopwords=sentenceWithoutStopwords.concat(word+" ");
                }
            }
            
            // emit the pair (null, sentenceWithoutStopwords)
            context.write(NullWritable.get(), new Text(sentenceWithoutStopwords));
    }
}
