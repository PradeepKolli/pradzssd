package com.jpmc.mapreduce;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class LzoSampleDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Configuration conf = getConf();
		
		//Comment these two while running locally. 
		conf.setBoolean("mapred.output.compress", true);
		conf.set("mapred.output.compression.codec","com.hadoop.compression.lzo.LzopCodec");

		Job job = Job.getInstance(conf, "Case Count");
		job.setJarByClass(LzoSampleDriver.class);

		FileUtils.deleteDirectory(new File(args[1]));

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(LzoSampleMapper.class);
		
		//This job does not need a reduce phase. The output could be achieved
		//using a Map Only job.
		job.setNumReduceTasks(0);

		job.waitForCompletion(true);

		if (job.isSuccessful()) {
			return 0;
		} else {
			return 1;
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new LzoSampleDriver(), args);
		System.exit(exitCode);
	}

}
