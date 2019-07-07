rm -rf ex35_out

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise35.SparkDriver --deploy-mode client --master local target/Exercise35_Dataset-1.0.0.jar "ex35_data/sensors.txt" "ex35_out/"


