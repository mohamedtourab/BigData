# Remove folders of the previous run
hdfs dfs -rm -r ex17_data_path1
hdfs dfs -rm -r ex17_data_path2
hdfs dfs -rm -r ex17_out

# Put input data collection into hdfs
hdfs dfs -put ex17_data_path1
hdfs dfs -put ex17_data_path2

# Run application
hadoop jar target/Exercise17-1.0.0.jar it.polito.bigdata.hadoop.exercise17.DriverBigData 1 ex17_data_path1/file1.txt ex17_data_path2/file2.txt ex17_out



