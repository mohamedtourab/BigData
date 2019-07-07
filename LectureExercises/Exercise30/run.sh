# Remove folders of the previous run
hdfs dfs -rm -r ex30_data
hdfs dfs -rm -r ex30_out

# Put input data collection into hdfs
hdfs dfs -put ex30_data

# Run application
spark2-submit  --class it.polito.bigdata.spark.exercise30.SparkDriver --deploy-mode cluster --master yarn target/Exercise30-1.0.0.jar "ex30_data/log.txt" ex30_out/


