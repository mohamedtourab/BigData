# Remove folders of the previous run
hdfs dfs -rm -r ex22_data
hdfs dfs -rm -r ex22_out

# Put input data collection into hdfs
hdfs dfs -put ex22_data


# Run application
hadoop jar target/Exercise22-1.0.0.jar it.polito.bigdata.hadoop.exercise22.DriverBigData ex22_data/  ex22_out/ User2



