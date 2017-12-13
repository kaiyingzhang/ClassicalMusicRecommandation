package com.assignment.ipRequest;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class linkFilteringReducer extends Reducer<Text,Text,Text,Text> {
    private IntWritable result = new IntWritable();
    private StringBuffer links = new StringBuffer("");
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      int sum = 0;


      for (Text val : values) {
    	  links.append(val+" ");
      }

      context.write(key, new Text(links.toString()));
      /*
        for (Text val : values) {
            context.write(key, new Text(val));
        }*/
    }
}