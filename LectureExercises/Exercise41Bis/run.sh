# Remove folders of the previous run
hdfs dfs -rm -r ex41_data
hdfs dfs -rm -r ex41_out

# Put input data collection into hdfs
hdfs dfs -put ex41_data

k=1

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise41bis.SparkDriver --deploy-mode cluster --master yarn target/Exercise41Bis-1.0.0.jar ${k} "ex41_data/sensors.txt" ex41_out


