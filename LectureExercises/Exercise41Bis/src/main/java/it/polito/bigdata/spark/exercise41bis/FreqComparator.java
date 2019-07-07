package it.polito.bigdata.spark.exercise41bis;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

@SuppressWarnings("serial")
public class FreqComparator implements Comparator<Tuple2<String, Integer>>, Serializable {

	public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
		// The comparison is based on the integer value of the tuple
		return -1 * (o1._2().compareTo(o2._2()));
	}

}
