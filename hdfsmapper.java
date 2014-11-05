package com.nrg.mr.regress.model;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Model5PMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text custId = new Text();
	private Text points = new Text();
	
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException
	{
		String line = value.toString();
		String[] tokens = line.split("\\|");
		String customerId;
		String dataPoints;
		if(tokens[1] == null || tokens[2] == null || tokens[3] == null)
		{
			return;
		}
		
		customerId = tokens[1];
		dataPoints = tokens[2]+":"+tokens[3];
		custId.set(customerId);
		points.set(dataPoints);
		context.write(custId, points);
	}
}
