package com.revature.jdouglas.driver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.jdouglas.map.MaleLaborForceAdvancedEdMapper;
import com.revature.jdouglas.reduce.MaleLaborForceAdvancedEdReducer;

public class MaleLaborForceAdvancedEdDriver {
	
public static void main(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.out.printf(
					"Usage: FemaleLaborForceAdvancedEd <input dir> <output dir>\n");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(PercentMaleDriver.class);
		job.setJobName("Female Labor Force");
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(MaleLaborForceAdvancedEdMapper.class);
		job.setReducerClass(MaleLaborForceAdvancedEdReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
		
	}

}
