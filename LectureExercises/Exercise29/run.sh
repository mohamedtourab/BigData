# Remove folders of the previous run
hdfs dfs -rm -r ex29_data_path1
hdfs dfs -rm -r ex29_data_path2
hdfs dfs -rm -r ex29_out

# Put input data collection into hdfs
hdfs dfs -put ex29_data_path1
hdfs dfs -put ex29_data_path2

# Run application
hadoop jar target/Exercise29-1.0.0.jar it.polito.bigdata.hadoop.exercise29.DriverBigData 1 ex29_data_path1/users.csv ex29_data_path2/likes.csv ex29_out



