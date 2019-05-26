package it.polito.bigdata.hadoop.exercise8;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.amazonaws.thirdparty.ion.IonException;

/**
 * Exercise 8 - Mapper
 */

class MapperBigData extends
		Mapper<Text, Text, Text, MonthIncome>{
	
	public void map(Text key,Text value,Context context) throws IOException,InterruptedException{
		
		String [] date = key.toString().split("-");
		
		MonthIncome monthIncome = new MonthIncome();
		
		monthIncome.setIncome(Double.parseDouble(value.toString()));
		monthIncome.setMonthID(date[1]);
		
		context.write(new Text(date[0]), monthIncome);
	}
}