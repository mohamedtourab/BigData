rm -rf ex34_out

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise34.SparkDriver --deploy-mode client --master local target/Exercise34_Dataset-1.0.0.jar "ex34_data/sensors.txt" "ex34_out/"


