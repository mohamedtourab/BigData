# Remove folders of the previous run
hdfs dfs -rm -r ex34_data
hdfs dfs -rm -r ex34_out

# Put input data collection into hdfs
hdfs dfs -put ex34_data


# Run application
spark2-submit  --class it.polito.bigdata.spark.exercise34.SparkDriver --deploy-mode client --master yarn target/Exercise34_Dataset-1.0.0.jar "ex34_data/sensors.txt" "ex34_out/"


