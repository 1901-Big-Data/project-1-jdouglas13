package com.revature.jdouglas.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageIncreaseFemaleEdMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		
		String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
		
		List<String> row = new ArrayList<String>();
		List<Double> percents = new ArrayList<Double>();
		
		for(String columnValue : columns)
			row.add(columnValue.replaceAll("\"", ""));
		
		String yearsPercent = "";
		
		String countryName = "";
	
		if(row.get(0).matches("United States") && row.get(3).matches("SE.TER.HIAT.BA.FE.ZS")) {
			countryName = row.get(0);
			for(int i = 44; i < 61; i++){
				if(row.get(i).isEmpty()) {
					continue;
				} else {
					yearsPercent = row.get(i).toString();
					percents.add(Double.parseDouble(yearsPercent));
				}	
			}
		} else {
			return;
		}
		
		for(Double percent: percents) {
			context.write(new Text(countryName), new DoubleWritable(percent));
		}
		
	}
}
