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
		// |-- name: string (nullable = true)
		// |-- surname: string (nullable = true)
		// |-- rangeage: String (nullable = true)
		Dataset<ProfileRangeAge> profilesDiscretizedAge = profilesDS.map(profile -> {
			ProfileRangeAge newProfile = new ProfileRangeAge();
			newProfile.setName(profile.getName());
			newProfile.setSurname(profile.getSurname());

			int min = (profile.getAge() / 10) * 10;
			int max = min + 1;

			newProfile.setRangeage(new String("[" + min + "-" + max + "]"));
			return newProfile;
		}, Encoders.bean(ProfileRangeAge.class));

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
