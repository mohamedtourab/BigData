# Remove folders of the previous run
rm -rf ex_out

# Run application
spark-submit  --class it.polito.bigdata.spark.Exercise47.SparkDriver --deploy-mode client --master local[10] target/Exercise47-1.0.0.jar "ex_data/stations.csv" ex_out


