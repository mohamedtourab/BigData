# Remove folders of the previous run
hdfs dfs -rm -r ex42_data_answers
hdfs dfs -rm -r ex42_data_questions

hdfs dfs -rm -r ex42_out

# Put input data collection into hdfs
hdfs dfs -put ex42_data_answers
hdfs dfs -put ex42_data_questions


# Run application
spark-submit  --class it.polito.bigdata.spark.exercise42.SparkDriver --deploy-mode cluster --master yarn target/Exercise42-1.0.0.jar "ex42_data_questions/questions.txt" "ex42_data_answers/answers.txt" ex42_out


