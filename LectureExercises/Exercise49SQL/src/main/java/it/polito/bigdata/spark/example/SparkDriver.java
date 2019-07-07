package it.polito.bigdata.spark.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a Spark Session object and set the name of the application
		SparkSession ss = SparkSession.builder().appName("Spark Exercise 49 - DataFrame").getOrCreate();

		// Read the content of the input file profiles.csv and store it into a
		// Dataset<profile>
		// The input file has an header
		// Schema of the input data:
		// |-- name: string (nullable = true)
		// |-- surname: string (nullable = true)
		// |-- age: integer (nullable = true)
		Dataset<Row> profilesDF = ss.read().format("csv").option("header", true).option("inferSchema", true)
				.load(inputPath);

		// Assign the “table name” profiles to the profilesDS DataFrame
		profilesDF.createOrReplaceTempView("profiles");

		// Define a User Defined Function called AgeCategory(Integer age)
		// that returns a string associated with the Category of the user.
		// AgeCategory = "[(age/10)*10-(age/10)*10+9]"
		// e.g.,
		// 43 -> [40-49]
		// 39 -> [30-39]
		// 21 -> [20-29]
		// 17 -> [10-19]
		// ..

		ss.udf().register("AgeCategory", (Integer age) -> {
			int min = (age / 10) * 10;
			int max = min + 1;
			return new String("[" + min + "-" + max + "]");
		}, DataTypes.StringType);

		// Define a DataFrame with the following schema based on the content of profiles:
		// |-- name: string (nullable = true)
		// |-- surname: string (nullable = true)
		// |-- rangeage: String (nullable = true)

		Dataset<Row> profilesDiscretizedAge = ss
				.sql("SELECT name, surname, AgeCategory(age) as rangeage FROM profiles");

		// Save the result in the output folder
		// To save the results in one single file, we use the repartition method
		// to associate the Dataframe with one single partition (by setting the
		// number of
		// partition to 1).
		profilesDiscretizedAge.repartition(1).write().format("csv").option("header", true).save(outputPath);

		// Close the Spark session
		ss.stop();

	}
}
