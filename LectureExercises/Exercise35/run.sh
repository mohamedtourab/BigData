# Remove folders of the previous run
hdfs dfs -rm -r ex35_data
hdfs dfs -rm -r ex35_out

# Put input data collection into hdfs
hdfs dfs -put ex35_data

# Run application
spark2-submit  --class it.polito.bigdata.spark.exercise35.SparkDriver --deploy-mode cluster --master yarn target/Exercise35-1.0.0.jar "ex35_data/sensors.txt" ex35_out


