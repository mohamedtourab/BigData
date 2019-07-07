# Remove folders of the previous run
rm -rf ex_out

# Run application
spark-submit  --class it.polito.bigdata.spark.Exercise51.SparkDriver --deploy-mode client --master local[10] target/Exercise51-1.0.0.jar ex_out

