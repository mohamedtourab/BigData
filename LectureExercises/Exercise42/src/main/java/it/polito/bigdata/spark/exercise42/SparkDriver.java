package it.polito.bigdata.spark.exercise42;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPathQuestions;
		String inputPathAnswers;
		String outputPath;

		inputPathQuestions = args[0];
		inputPathAnswers = args[1];
		outputPath = args[2];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #42");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the question file
		JavaRDD<String> questionsRDD = sc.textFile(inputPathQuestions);

		// Create a PairRDD with the questionId as key and the question text
		// as value
		JavaPairRDD<String, String> questionsPairRDD = questionsRDD.mapToPair(question -> {

			String questionID;
			String questionText;
			Tuple2<String, String> pair;

			// Split the line in fields
			String[] fields = question.split(",");

			// fields[0] contains the questionId
			questionID = fields[0];

			// fields[2] contains the text of the question
			questionText = fields[2];

			pair = new Tuple2<String, String>(questionID, questionText);

			return pair;
		});

		// Read the content of the answer file
		JavaRDD<String> answersRDD = sc.textFile(inputPathAnswers);

		// Create a PairRDD with the questionId as key and the answer text
		// as value
		JavaPairRDD<String, String> answersPairRDD = answersRDD.mapToPair(answer -> {

			String questionID;
			String questionText;
			Tuple2<String, String> pair;

			// Split the line in fields
			String[] fields = answer.split(",");

			// fields[1] contains the questionId
			questionID = fields[1];

			// fields[3] contains the text of the answer
			questionText = fields[3];

			pair = new Tuple2<String, String>(questionID, questionText);

			return pair;
		});

		// "Cogroup" the two PairRDDs
		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> questionsAnswersPairRDD = 
				questionsPairRDD.cogroup(answersPairRDD);

		questionsAnswersPairRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
