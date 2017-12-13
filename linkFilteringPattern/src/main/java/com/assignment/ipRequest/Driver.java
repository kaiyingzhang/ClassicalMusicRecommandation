package com.assignment.ipRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
			System.exit(-1);
		}

		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		// Create configuration
		Configuration conf = new Configuration(true);

		// Create job
		Job job = new Job(conf, "LinkFilteringSorting");
		job.setJarByClass(linkFilteringMapper.class);
		

		// Setup MapReduce

		job.setMapperClass(linkFilteringMapper.class);
		job.setReducerClass(linkFilteringReducer.class);

		job.setNumReduceTasks(1);

		// Specify key / value
	
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//MultipleOutputs.addNamedOutput(job, TextOutputFormat.class, Text.class, Text.class);
		//MultipleOutputs.addNamedOutput(job, "bins",TextOutputFormat.class, Text.class, NullWritable.class);
		//MultipleOutputs.setCountersEnabled(job, true);
		
		job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(IntWritable.class);
		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		//conf.setInt(MRJobConfig.COUNTERS_MAX_KEY, 6000);
		 //Limits.init(conf);
		// Output
		FileOutputFormat.setOutputPath(job, outputDir);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		// Execute job
		int code = job.waitForCompletion(true) ? 0 : 1;

		System.exit(code);

	}

}