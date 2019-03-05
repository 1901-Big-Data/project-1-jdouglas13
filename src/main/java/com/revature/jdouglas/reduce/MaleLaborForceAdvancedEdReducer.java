package com.revature.jdouglas.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaleLaborForceAdvancedEdReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>  {

	@Override
	protected void reduce(Text arg0, Iterable<DoubleWritable> arg1, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context arg2) throws IOException, InterruptedException {
		
		List<Double> percents = new ArrayList<Double>();
		
		for (DoubleWritable percent: arg1) {
			percents.add(new Double(percent.get()));
		}
		
		Double percentsTotal = 0.0;
		
		for (Double decimalNumber: percents){
			percentsTotal += decimalNumber;
		}
		
		Double calculatedPercentage = (double) Math.round((percentsTotal / percents.size()) * 100 / 100);
		
		arg2.write(new Text(arg0), new DoubleWritable(calculatedPercentage));
		
	}
}
