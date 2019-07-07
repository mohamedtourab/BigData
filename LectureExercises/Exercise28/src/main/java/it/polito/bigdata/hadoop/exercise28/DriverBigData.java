//				EXERCISE 28
/**
 * Driver class.
 */
public class DriverBigData extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    Path inputPath1, inputPath2;
    Path outputDir;
    int numberOfReducers;
	int exitCode;  
	
	// Parse the parameters
    numberOfReducers = Integer.parseInt(args[0]);
    inputPath1 = new Path(args[1]);
    inputPath2 = new Path(args[2]);
    outputDir = new Path(args[3]);
    
    Configuration conf = this.getConf();

    // Define a new job
    Job job = Job.getInstance(conf); 

    // Assign a name to the job
    job.setJobName("Exercise 28");
    
    // Set two input paths and two mapper classes
    MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, MapperType1BigData.class);
    MultipleInputs.addInputPath(job, inputPath2, TextInputFormat.class, MapperType2BigData.class);
    
    // Set path of the output folder for this job
    FileOutputFormat.setOutputPath(job, outputDir);
    
    // Specify the class of the Driver for this job
    job.setJarByClass(DriverBigData.class);
    
    
    // Set job output format
    job.setOutputFormatClass(TextOutputFormat.class);
    
    
    // Set map output key and value classes
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    // Set reduce class
    job.setReducerClass(ReducerBigData.class);
        
    // Set reduce output key and value classes
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    // Set number of reducers
    job.setNumReduceTasks(numberOfReducers);
    
    // Execute the job and wait for completion
    if (job.waitForCompletion(true)==true)
    	exitCode=0;
    else
    	exitCode=1;
    	
    return exitCode;
  }

  /** Main of the driver
   */
  
  public static void main(String args[]) throws Exception {
	// Exploit the ToolRunner class to "configure" and run the Hadoop application
    int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);

    System.exit(res);
  }
}


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
