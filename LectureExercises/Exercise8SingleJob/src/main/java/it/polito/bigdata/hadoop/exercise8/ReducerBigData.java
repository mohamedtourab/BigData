package it.polito.bigdata.hadoop.exercise8;
	

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * Exercise 8 - Reducer
 */
class ReducerBigData extends
		Reducer<Text, MonthIncome, Text, DoubleWritable>{
	
	@Override	
	public void reduce(Text key,Iterable<MonthIncome> values,Context context)throws IOException,InterruptedException {
		
		String year = key.toString();
		HashMap<String, Double> hashMap = new HashMap<String, Double>();
		
		int count = 0;
		double totalYearlyIncome = 0;
		
		for(MonthIncome value:values) {
			
			Double income = hashMap.get(value.getMonthID());
			
			if(income == null) {
				hashMap.put(new String(value.getMonthID()),new Double(value.getIncome()));
				count++;
			}
			else {
				hashMap.put(new String(value.getMonthID()) , new Double(income+value.getIncome()));
			}
			totalYearlyIncome = totalYearlyIncome + value.getIncome();
			
			for(Entry<String, Double> pair:hashMap.entrySet()) {
				context.write(new Text(year+"-"+pair.getKey()), new DoubleWritable(pair.getValue()));
			}
		}
		context.write(new Text(year), new DoubleWritable(totalYearlyIncome/count));
		
	}
}