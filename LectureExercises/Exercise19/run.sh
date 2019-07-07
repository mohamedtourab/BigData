# Remove folders of the previous run
hdfs dfs -rm -r ex19_data
hdfs dfs -rm -r ex19_out

# Put input data collection into hdfs
hdfs dfs -put ex19_data

# Run application
hadoop jar target/Exercise19-1.0.0.jar it.polito.bigdata.hadoop.exercise20.DriverBigData ex19_data ex19_out



