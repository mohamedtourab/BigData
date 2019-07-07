# Remove folders of the previous run
hdfs dfs -rm -r ex20_data
hdfs dfs -rm -r ex20_out

# Put input data collection into hdfs
hdfs dfs -put ex20_data

# Run application
hadoop jar target/Exercise20-1.0.0.jar it.polito.bigdata.hadoop.exercise20.DriverBigData ex20_data ex20_out



