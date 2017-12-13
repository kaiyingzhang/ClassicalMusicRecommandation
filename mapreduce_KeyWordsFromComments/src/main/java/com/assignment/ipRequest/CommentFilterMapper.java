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


//key_value inverted pattern
public class CommentFilterMapper extends Mapper<LongWritable, Text, Text, Text> {


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String info[] = value.toString().split(",");
		if(info.length >= 3){
			String link = info[1].replace(" ","");
			String name = info[0];
			String com = value.toString().replace(info[0],"");
			String comment = com.toString().replace(info[1],"");
			comment = comment.toString().replace(",","");
			System.err.println("..."+link.length());
			if(link.length() == 11) {
				context.write(new Text(link + "@@@" + name + "@@@"), new Text(comment));
			}
		}



	}
	

}

