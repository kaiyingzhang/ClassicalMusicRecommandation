package com.assignment.ipRequest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TopReducer extends Reducer<IntWritable, Text, Text, Text> {

    public static int top = 0;
    public void reduce(IntWritable key, Iterable<Text> values,Context context)
            throws IOException, InterruptedException {


            for(Text val : values){

                if(top<=10){
                    String info[] = val.toString().split("::");
                context.write(new Text(info[0].toString()), new Text(info[1].toString()));
                top++;

            }
        }



    }
}