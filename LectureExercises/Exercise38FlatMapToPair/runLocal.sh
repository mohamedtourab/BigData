# Remove folders of the previous run
rm -rf ex38_out

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise38.SparkDriver --deploy-mode client --master local target/Exercise38FlatMapToPair-1.0.0.jar "ex38_data/sensors.txt" ex38_out


