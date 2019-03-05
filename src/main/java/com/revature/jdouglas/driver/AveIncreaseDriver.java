package com.revature.jdouglas.driver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.jdouglas.map.AverageIncreaseFemaleEdMapper;
import com.revature.jdouglas.reduce.AverageIncreaseFemaleEdReducer;

public class AveIncreaseDriver {

public static void main(String[] args) throws Exception {
		
		Job job = new Job();
		job.setJarByClass(AveIncreaseDriver.class);
		job.setJobName("Average Increase Female Ed Stat");
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(AverageIncreaseFemaleEdMapper.class);
		job.setReducerClass(AverageIncreaseFemaleEdReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
