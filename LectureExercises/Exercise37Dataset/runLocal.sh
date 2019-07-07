rm -rf ex37_out

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise37.SparkDriver --deploy-mode client --master local target/Exercise37_Dataset-1.0.0.jar "ex37_data/sensors.txt" "ex37_out/"


