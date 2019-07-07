# Remove folders of the previous run
hdfs dfs -rm -r ex28_data_path1
hdfs dfs -rm -r ex28_data_path2
hdfs dfs -rm -r ex28_out

# Put input data collection into hdfs
hdfs dfs -put ex28_data_path1
hdfs dfs -put ex28_data_path2

# Run application
hadoop jar target/Exercise28-1.0.0.jar it.polito.bigdata.hadoop.exercise28.DriverBigData 2 ex28_data_path1/questions.txt ex28_data_path2/answers.txt ex28_out



