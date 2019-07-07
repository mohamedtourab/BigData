# Remove folders of the previous run
hdfs dfs -rm -r ex21_data
hdfs dfs -rm -r ex21_out
# Remove the stopword file from hdfs
hdfs dfs -rm stopwords.txt

# Put input data collection into hdfs
hdfs dfs -put ex21_data

# Remove the stopword file in hdfs
hdfs dfs -put stopwords.txt

# Run application
hadoop jar target/Exercise21-1.0.0.jar it.polito.bigdata.hadoop.exercise21.DriverBigData ex21_data ex21_out



