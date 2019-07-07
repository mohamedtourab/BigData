# Remove folders of the previous run
hdfs dfs -rm -r ex25_data
hdfs dfs -rm -r ex25_out
hdfs dfs -rm -r ex25_out2

# Put input data collection into hdfs
hdfs dfs -put ex25_data


# Run application
hadoop jar target/Exercise25-1.0.0.jar it.polito.bigdata.hadoop.exercise25.DriverBigData 1 ex25_data/  ex25_out/ ex25_out2/ User2 



