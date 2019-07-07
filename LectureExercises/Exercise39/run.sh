# Remove folders of the previous run
hdfs dfs -rm -r ex39_data
hdfs dfs -rm -r ex39_out

# Put input data collection into hdfs
hdfs dfs -put ex39_data

# Run application
spark2-submit  --class it.polito.bigdata.spark.exercise39.SparkDriver --deploy-mode cluster --master yarn target/Exercise39-1.0.0.jar "ex39_data/sensors.txt" ex39_out


