package com.revature.jdouglas.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PercentChangeMaleEmploymentMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
String line = value.toString();
		
		String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
		
		List<String> row = new ArrayList<String>();
		List<Double> percents = new ArrayList<Double>();
		
		for(String columnValue : columns)
			row.add(columnValue.replaceAll("\"", ""));
		
		String countryName = "";
		
		if (line.contains("SL.EMP.TOTL.SP.MA.NE.ZS")) {
			countryName = row.get(0).toString();
		} else {
			return;
		}
		
		String yearsPercent = "";
		
		for(int i = 44; i < 61; i++){
			if(row.get(i).isEmpty()) {
				continue;
			} else {
				yearsPercent = row.get(i);
				percents.add(Double.parseDouble(yearsPercent));
			}
		}
		
		for(Double percent: percents) {
			context.write(new Text(countryName), new DoubleWritable(percent));
		}
		
	}
		
}
