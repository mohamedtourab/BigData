# Remove folders of the previous run
hdfs dfs -rm -r ex33_data

# Put input data collection into hdfs
hdfs dfs -put ex33_data

# Run application
spark2-submit  --class it.polito.bigdata.spark.exercise33.SparkDriver --deploy-mode client --master yarn target/Exercise33_SQL-1.0.0.jar "ex33_data/sensors.txt"


