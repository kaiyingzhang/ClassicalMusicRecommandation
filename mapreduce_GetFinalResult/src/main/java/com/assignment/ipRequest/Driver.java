package com.assignment.ipRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Driver {

	public static void main(String[] args) throws Exception {
		if (args.length != 5) {
			System.err.println("Usage: WordCount <in> <out>");
			System.exit(3);
		}

	Path inputPath1 = new Path(args[0]);
	Path inputPath2 = new Path(args[1]);
	//Path inputPath3 = new Path(args[2]);
	//String inputPath2 = (args[1]+"\\CommentsIndex.txt").toString();
	//String inputPath3 = (args[2]+"\\BX-Books.csv").toString();

	Path outputDir = new Path(args[2]);
	String outputFile = (args[2]+"\\part-m-00000").toString();
	//Path second_input_path = outputDir;
	Path Counter_output = new Path(args[3]);
	Path final_output = new Path(args[4]);

	Configuration conf = new Configuration(true);

		Job job = new Job(conf, "Replicate Join");
		//job.getConfiguration().set("userInformation.path",inputPath2);

		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper1.class);
		//job.setReducerClass(Reducer1.class);

		job.setNumReduceTasks(0);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
	// Input
		FileInputFormat.addInputPath(job, inputPath1);
		job.setInputFormatClass(TextInputFormat.class);

	// Output
		FileOutputFormat.setOutputPath(job, outputDir);

	// Delete output if exists
	FileSystem hdfs  = FileSystem.get(conf);
	FileSystem local = FileSystem.getLocal(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);
		if (hdfs.exists(Counter_output))
			hdfs.delete(Counter_output, true);
		if (hdfs.exists(final_output))
			hdfs.delete(final_output, true);

	boolean complete = job.waitForCompletion(true);

	Configuration conf2 = new Configuration();
	Job job2 = Job.getInstance(conf2, "chaining");

		job2.getConfiguration().set("TextIndexInformation.path",outputFile);
		if (complete) {
		//if(false){
		job2.setJarByClass(Driver.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setNumReduceTasks(1);


		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputValueClass(Text.class);
		job2.setOutputKeyClass(IntWritable.class);


		FileInputFormat.addInputPath(job2, inputPath2);
		FileOutputFormat.setOutputPath(job2, Counter_output);

	}

		boolean complete2 = job2.waitForCompletion(true);
		Configuration conf3 = new Configuration();
		Job job3 = Job.getInstance(conf3, "chaining");

		if (complete2) {
			//if(false){
			job3.setJarByClass(Driver.class);
			job3.setMapperClass(TopMapper.class);
			job3.setReducerClass(TopReducer.class);
			job3.setNumReduceTasks(1);

			job3.setSortComparatorClass(IntComparator.class);

			job3.setMapOutputKeyClass(IntWritable.class);
			job3.setMapOutputValueClass(Text.class);
			job3.setOutputValueClass(Text.class);
			job3.setOutputKeyClass(IntWritable.class);


			FileInputFormat.addInputPath(job3, Counter_output);
			FileOutputFormat.setOutputPath(job3, final_output);

			System.exit(job3.waitForCompletion(true) ? 0 : 1);
		}


}


}