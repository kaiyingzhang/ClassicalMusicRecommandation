package com.assignment.ipRequest;


import java.io.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class Mapper1 extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {


	//private final static IntWritable one = new IntWritable(1);
	private DoubleWritable num = new DoubleWritable();
	private Text num1 = new Text();
	private String n = "1";

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		System.err.println("!!!!!!");
		String line = value.toString();
		//String[] tokens= line.split(",");
		String[] tokens = new String[7];
		tokens= line.split(",");
		String Artist = tokens[0].toString();
		 String SongName = tokens[1].toString().replace("/"," ").trim();
		String SentimentScore = tokens[2].toString().replace(" ","").trim();
		String JoyScore = tokens[3].toString().replace(" ","").trim();
		String SorrowScore = tokens[4].toString().replace(" ","").trim();
		String AngryScore = tokens[5].toString().replace(" ","").trim();
		String SurpriseScore = tokens[6].toString().replace(" ","").trim();

		String Keyword = tokens[7].toString();

		String[] keywords = Keyword.split(" ");
		// System.out.print(keywords[0]);

		context.write(new Text("positiveornegative"+SentimentScore), new Text(""));
		context.write(new Text("Joy"+JoyScore), new Text(""));
		context.write(new Text("Sorrow"+SorrowScore), new Text(""));
		context.write(new Text("Angry"+AngryScore), new Text(""));
		context.write(new Text("Surprise"+SurpriseScore), new Text(""));

		for(int index = 1; index < keywords.length; index++)//
		{
			String word = keywords[index].toString().trim().toLowerCase();
			context.write(new Text(word), new Text(""));
		}

	}

}

