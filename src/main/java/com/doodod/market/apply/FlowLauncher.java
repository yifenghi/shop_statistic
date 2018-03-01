package com.doodod.market.apply;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import com.doodod.market.statistic.Common;

public class FlowLauncher extends Configured implements Tool{
	public int run(String[] args) throws Exception {
		GenericOptionsParser options = new GenericOptionsParser(getConf(), args);
		Configuration hadoopConf = options.getConfiguration();

		Job job = new Job(hadoopConf);
		job.setJarByClass(FlowLauncher.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setReducerClass(FlowReducer.class);
		
		MultipleInputs.addInputPath(job, new Path(
				hadoopConf.get(Common.FLOW_INPUT_MIN)), 
				SequenceFileInputFormat.class, FlowMinMapper.class);
		MultipleInputs.addInputPath(job, new Path(
				hadoopConf.get(Common.FLOW_INPUT_HOUR)), 
				SequenceFileInputFormat.class, FlowHourMapper.class);
		MultipleInputs.addInputPath(job, new Path(
				hadoopConf.get(Common.FLOW_INPUT_DAY)),
				SequenceFileInputFormat.class, FlowDayMapper.class);
		
		job.waitForCompletion(true);
		return 0;	
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(
				new Configuration(), new FlowLauncher(), args));
	}

}
