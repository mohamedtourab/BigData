package it.polito.bigdata.spark.exercise42;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class QuestionIdAnswerText implements PairFunction<String, String, String> {
	// Implement the call method
	// Return a pair (questionId, answerText)
	public Tuple2<String, String> call(String answer) {

		String questionID;
		String questionText;
		Tuple2<String, String> pair;
		
		// Split the line in fields
		String[] fields=answer.split(",");

		// fields[1] contains the questionId
		questionID=fields[1];

		// fields[3] contains the text of the answer
		questionText=fields[3];

		pair=new Tuple2<String, String>(questionID, questionText);
		
		return pair; 
	}
}
