# Remove folders of the previous run
rm -rf ex30_out

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise30.SparkDriver --deploy-mode client --master local target/Exercise30-1.0.0.jar "ex30_data/log.txt" ex30_out/


