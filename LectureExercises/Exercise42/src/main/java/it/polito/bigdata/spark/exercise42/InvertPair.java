package it.polito.bigdata.spark.exercise42;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class InvertPair implements PairFunction<Tuple2<String, Integer>, Integer, String> {
	// Implement the call method
	// The call method receives one reading/one record 
	// and returns a pair (sensorID, 1)
	public Tuple2<Integer, String> call(Tuple2<String, Integer> inPair) {

		Tuple2<Integer, String> invertedPair;

		// Invert key with value and value with key 
		invertedPair=new Tuple2<Integer, String>(inPair._2(), inPair._1());
		
		return invertedPair; 
	}
}
