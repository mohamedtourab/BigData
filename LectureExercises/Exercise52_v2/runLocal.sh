# Remove folders of the previous run
rm -rf ex_out

# Run application
spark-submit  --class it.polito.bigdata.spark.Exercise52.SparkDriver --deploy-mode client --master local[10] target/Exercise52_v2-1.0.0.jar historicalData.txt ex_out

