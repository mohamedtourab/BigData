# Remove folders of the previous run
hdfs dfs -rm -r ex37_data
hdfs dfs -rm -r ex37_out

# Put input data collection into hdfs
hdfs dfs -put ex37_data

# Run application
spark2-submit  --class it.polito.bigdata.spark.exercise32.SparkDriver --deploy-mode client --master yarn target/Exercise37_SQL-1.0.0.jar "ex37_data/sensors.txt" "ex37_out/"


