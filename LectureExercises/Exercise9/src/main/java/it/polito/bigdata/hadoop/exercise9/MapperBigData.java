package it.polito.bigdata.hadoop.exercise9;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 9 - Mapper
 */
class MapperBigData extends
		Mapper<	LongWritable, 
				Text, 
				Text, 
				IntWritable>{
	
	HashMap<String, Integer> hashMap;
	
	
	protected void setup(Context context) {
		hashMap = new HashMap<String, Integer>();
	}
	
	
	public void map(LongWritable key,Text value, Context context)
					throws IOException, InterruptedException{
		
		 
		String [] words = value.toString().split("\\s+");
		Integer occurrences = 0;
	
		for(String word:words) {
			
			word = word.toLowerCase();
			
			occurrences = hashMap.get(word);
			
			if(occurrences == null) {
				hashMap.put(word, 1);
			}
			else {
				hashMap.put(word, occurrences+1);
			}
		}
		
	}	
	
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for(Entry<String, Integer> pair:hashMap.entrySet()) {
			context.write(new Text(pair.getKey()), new IntWritable(pair.getValue()));
		}
	}

}