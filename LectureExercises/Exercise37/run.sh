# Remove folders of the previous run
hdfs dfs -rm -r ex37_data
hdfs dfs -rm -r ex37_out

# Put input data collection into hdfs
hdfs dfs -put ex37_data

# Run application
spark2-submit  --class it.polito.bigdata.spark.exercise37.SparkDriver --deploy-mode cluster --master yarn target/Exercise37-1.0.0.jar "ex37_data/sensors.txt" ex37_out


