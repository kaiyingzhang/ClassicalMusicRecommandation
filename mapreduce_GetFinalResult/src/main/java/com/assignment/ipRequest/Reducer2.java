package com.assignment.ipRequest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class Reducer2 extends org.apache.hadoop.mapreduce.Reducer<Text, Text, IntWritable, Text> {

    public void reduce(Text key, Iterable<Text> values,Context context)
            throws IOException, InterruptedException {
        int count = 0;
        //  System.out.print(key+",");
        for(Text val: values) {
            count++;
        }

        context.write(new IntWritable(count), new Text(":"+key));

    }
}