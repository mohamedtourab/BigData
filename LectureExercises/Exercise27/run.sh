# Remove folders of the previous run
hdfs dfs -rm -r ex27_data
hdfs dfs -rm -r ex27_out
# Remove the business rules file from hdfs
hdfs dfs -rm businessrules.txt

# Put input data collection into hdfs
hdfs dfs -put ex27_data

# Put the business rules file into hdfs
hdfs dfs -put businessrules.txt

# Run application
hadoop jar target/Exercise27-1.0.0.jar it.polito.bigdata.hadoop.exercise27.DriverBigData ex27_data ex27_out



