package com.assignment.ipRequest;


import java.io.*;


import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Mapper2 extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {

	//private static final Text EMPTY_TEXT = new Text("");
	private HashMap<String, String> userIdToInfo = new HashMap<String, String>();
	private String Text_Index_location = null;


	public void setup(Context context) throws IOException,InterruptedException {

		Text_Index_location = context.getConfiguration().get("TextIndexInformation.path");
		BufferedReader rdr = new BufferedReader(new FileReader(Text_Index_location));
			//BufferedReader rdr = new BufferedReader(new InputStreamReader((new GZIPInputStream(new FileInputStream(new File(p.toString()))))));

			String line = null;
			while ((line = rdr.readLine()) != null) {

				String TextIndex = line.replaceAll("\\s", "");
				userIdToInfo.put("["+TextIndex,line);
			}



	}
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


		String line[] = value.toString().split("]");
		//System.err.println(line[0]+"..."+line[1]+"..."+line.length);
		System.err.println("..."+value);

		String CommentIndex = line[0].replaceAll("\\s", "");
		String TextInfo = userIdToInfo.get(CommentIndex);
		if(TextInfo != null){

			//context.write(new Text(value),new Text((TextInfo.toString())));

			String[] link = value.toString().split("]\t");

			String[] links = link[1].toString().split("/");
			for(int i = 0;i<links.length;i++){

				System.err.println("!!!"+links[i]);
				context.write(new Text(links[i].toString().replaceAll("\\s", "")),new Text(""));
			}
		}else{
			//context.write(value,EMPTY_TEXT);
		}
		
	}


}

