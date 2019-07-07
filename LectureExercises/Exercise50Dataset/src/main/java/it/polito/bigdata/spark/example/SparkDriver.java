package it.polito.bigdata.spark.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

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
		Dataset<Profile> profilesDS = ss.read().format("csv").option("header", true).option("inferSchema", true)
				.load(inputPath).as(Encoders.bean(Profile.class));

		// Define a Dataset with the following schema:
		// |-- name_surname: string (nullable = true)
		Dataset<NameSurname> profilesDiscretizedAge = profilesDS.map(profile -> {
			NameSurname nameSurname = new NameSurname();
			nameSurname.setName_surname(new String(profile.getName() + " " + profile.getSurname()));

			return nameSurname;
		}, Encoders.bean(NameSurname.class));

		// Save the result in the output folder
		// To save the results in one single file, we use the repartition method
		// to associate the Dataset with one single partition (by setting the
		// number of
		// partition to 1).
		profilesDiscretizedAge.repartition(1).write().format("csv").option("header", true).save(outputPath);

		// Close the Spark session
		ss.stop();

	}
}
