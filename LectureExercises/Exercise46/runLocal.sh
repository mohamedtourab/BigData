# Remove folders of the previous run
rm -rf ex46_out

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise46.SparkDriver --deploy-mode client --master local target/Exercise46-1.0.0.jar "ex46_data/readings.txt" ex46_out

