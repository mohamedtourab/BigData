# Remove folders of the previous run
hdfs dfs -rm -r ex38_data
hdfs dfs -rm -r ex38_out

# Put input data collection into hdfs
hdfs dfs -put ex38_data


# Run application
spark2-submit  --class it.polito.bigdata.spark.exercise38.SparkDriver --deploy-mode client --master yarn target/Exercise38_Dataset-1.0.0.jar "ex38_data/sensors.txt" "ex38_out/"


