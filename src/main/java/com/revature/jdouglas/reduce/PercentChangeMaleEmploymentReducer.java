package com.revature.jdouglas.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PercentChangeMaleEmploymentReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	protected void reduce(Text arg0, Iterable<DoubleWritable> arg1, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context arg2) throws IOException, InterruptedException {
		
		List<Double> percents = new ArrayList<Double>();
		
		for (DoubleWritable percent: arg1) {
			percents.add(new Double(percent.get()));
		}
				
		Double percentIncrease = ((double) Math.round((((percents.get((percents.size()-1)) - percents.get(0)) / percents.get(0) * 100)) * 100) / 100);
		
		arg2.write(arg0, new DoubleWritable(percentIncrease));

	}
}
