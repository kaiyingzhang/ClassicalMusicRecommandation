package com.assignment.ipRequest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IntComparator extends WritableComparator {
	
	protected IntComparator() {
		  super(IntWritable.class, true);
		}
	  
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		  
		IntWritable v1 = (IntWritable)w1;
		IntWritable v2 = (IntWritable)w2;
		  int cmpResult = v2.compareTo(v1);
		  return cmpResult;
		  
	  }

}
