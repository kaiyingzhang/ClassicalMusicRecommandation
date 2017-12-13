package com.assignment.ipRequest;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;



public class linkFilteringMapper extends Mapper<LongWritable, Text, Text, Text> {


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String info[] = value.toString().split(",");
		String link = info[1].substring(33,44);
		String name = info[0]+":";

		context.write(new Text(name),new Text(link));

	}
	

}

