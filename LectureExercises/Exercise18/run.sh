# Remove folders of the previous run
hdfs dfs -rm -r ex18_data
hdfs dfs -rm -r ex18_out

# Put input data collection into hdfs
hdfs dfs -put ex18_data

# Run application
hadoop jar target/Exercise18-1.0.0.jar it.polito.bigdata.hadoop.exercise20.DriverBigData ex18_data ex18_out



