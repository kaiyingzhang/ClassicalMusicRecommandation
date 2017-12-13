package com.assignment.ipRequest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class Reducer1 extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        String value = "";
        String allValue = "";
        //  System.out.print(key+",");
        while (values.hasNext()) {
            value = values.next().toString();
            allValue += value + "/";

            allValue = allValue.toString().trim();
            String label = key.toString();

            output.collect(new Text("[" + label + "]"), new Text(allValue));
        }

    }
}