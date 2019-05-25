# Remove folders of the previous run
hdfs dfs -rm -r lab2_data
hdfs dfs -rm -r lab2_out

# Put input data collection into hdfs
hdfs dfs -put lab2_data


# Run application
hadoop jar target/Exercise4-1.0.0.jar it.polito.bigdata.hadoop.lab2.DriverBigData 1 "lab2_data/*.txt"  lab2_out



