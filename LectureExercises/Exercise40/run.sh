# Remove folders of the previous run
hdfs dfs -rm -r ex40_data
hdfs dfs -rm -r ex40_out

# Put input data collection into hdfs
hdfs dfs -put ex40_data

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise40.SparkDriver --deploy-mode cluster --master yarn target/Exercise40-1.0.0.jar "ex40_data/sensors.txt" ex40_out


