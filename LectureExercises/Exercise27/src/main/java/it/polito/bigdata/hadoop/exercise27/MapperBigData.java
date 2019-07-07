package it.polito.bigdata.hadoop.exercise27;

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
 * Exercise 27 - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type

	private ArrayList<String> rules;

	
	@SuppressWarnings("deprecation")
	protected void setup(Context context) throws IOException, InterruptedException
	{
		String nextLine;
	
		rules=new ArrayList<String>();
		// Open the business rules file (that is shared by means of the distributed 
		// cache mechanism) 
				
		Path[] PathsCachedFiles = context.getLocalCacheFiles();	
		
		// This application has one single single cached file. 
		// Its path is URIsCachedFiles[0] 
		BufferedReader rulesFile = new BufferedReader(new FileReader(new File(PathsCachedFiles[0].toString())));
	
		
		// Each line of the file contains one rule 
		while ((nextLine = rulesFile.readLine()) != null) {
			rules.add(nextLine);
		}
	
		rulesFile.close();
	}


	private String applyBusinessRule(String gender, String year)
	{
		String category=new String("Unknown");

		// Iterate over the rules
		for (String rule: rules)
		{
			// Gender=<value> and DateOfBirth=<value> -> Category
			String[] ruleParts=rule.split(" ");
			// ruleParts[0] = Gender=<value>
			// ruleParts[2] = DateOfBirth=<value>
			// ruleParts[4] = category
			// Check if the current rule is satisfied by the current user 
			if (ruleParts[0].compareTo("Gender="+gender)==0 && 
					ruleParts[2].compareTo("YearOfBirth="+year)==0)
				category=ruleParts[4];
		}
		
		return category;
	}
	
	
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		String category;

            // Split each record in fields
    		// UserId,Name,Surname,Gender,YearOfBirth,City,Education
            String[] fields = value.toString().split(",");

            category=applyBusinessRule(fields[3],fields[4]);
            
              
            // emit the pair (null, record+category)
            context.write(NullWritable.get(), new Text(value.toString()+","+category));
    }
}
