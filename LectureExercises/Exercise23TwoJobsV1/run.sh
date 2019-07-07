# Remove folders of the previous run
hdfs dfs -rm -r ex23_data
hdfs dfs -rm -r ex23_out

# Put input data collection into hdfs
hdfs dfs -put ex23_data


# Run application
hadoop jar target/Exercise23_V1-1.0.0.jar it.polito.bigdata.hadoop.exercise23other.DriverBigData 2 ex23_data/  ex23_out/ User2



