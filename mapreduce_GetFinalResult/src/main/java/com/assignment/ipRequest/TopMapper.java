package com.assignment.ipRequest;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class TopMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


		String line[] = value.toString().split("\t:");
		int count = Integer.valueOf(line[0].replaceAll("\\s",""));

		context.write(new IntWritable(count),new Text(line[1]));

	}


}

