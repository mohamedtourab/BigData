# Remove folders of the previous run
rm -rf ex_out

# Run application
spark-submit  --class it.polito.bigdata.spark.example.SparkDriver --deploy-mode client --master local target/Exercise49Dataset-1.0.0.jar "ex49_data/profiles.csv" ex_out


