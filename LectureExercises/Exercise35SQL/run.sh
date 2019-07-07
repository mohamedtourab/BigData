# Remove folders of the previous run
hdfs dfs -rm -r ex35_data
hdfs dfs -rm -r ex35_out

# Put input data collection into hdfs
hdfs dfs -put ex35_data

# Run application
spark2-submit  --class it.polito.bigdata.spark.exercise32.SparkDriver --deploy-mode client --master yarn target/Exercise35_SQL-1.0.0.jar "ex35_data/sensors.txt" "ex35_out/"


