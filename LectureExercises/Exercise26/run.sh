# Remove folders of the previous run
hdfs dfs -rm -r ex26_data
hdfs dfs -rm -r ex26_out
# Remove the stopword file from hdfs
hdfs dfs -rm dictionary.txt

# Put input data collection into hdfs
hdfs dfs -put ex26_data

# Remove the stopword file in hdfs
hdfs dfs -put dictionary.txt

# Run application
hadoop jar target/Exercise26-1.0.0.jar it.polito.bigdata.hadoop.exercise26.DriverBigData ex26_data ex26_out



