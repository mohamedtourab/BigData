# Remove folders of the previous run
hdfs dfs -rm -r ex24_data
hdfs dfs -rm -r ex24_out

# Put input data collection into hdfs
hdfs dfs -put ex24_data


# Run application
hadoop jar target/Exercise24-1.0.0.jar it.polito.bigdata.hadoop.exercise24.DriverBigData 1 ex24_data/  ex24_out/ 



