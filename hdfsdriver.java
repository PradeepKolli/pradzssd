package com.nrg.mr.regress.model;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.nrg.mr.DataStatsDriver;
import com.nrg.mr.DataStatsMapper;
import com.nrg.mr.DataStatsReducer;

public class Model5PDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Configuration conf = getConf();
		Job job = new Job(conf, "Enery Usage Data Statistics");

		job.setJarByClass(Model5PDriver.class);
		job.setMapperClass(Model5PMapper.class);
		job.setReducerClass(Model5PReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//job.setNumReduceTasks(1);

		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int exit = ToolRunner.run(new Model5PDriver(), args);
		System.exit(exit);
	}

}
