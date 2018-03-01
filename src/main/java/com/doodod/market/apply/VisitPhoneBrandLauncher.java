package com.doodod.market.apply;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.doodod.market.apply.VisitPhoneBrandLauncher;
import com.doodod.market.apply.VisitPhoneBrandMapper;
import com.doodod.market.apply.VisitPhoneBrandReducer;

public class VisitPhoneBrandLauncher extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		GenericOptionsParser options = new GenericOptionsParser(getConf(), args);
		Configuration hadoopConf = options.getConfiguration();
		
		Job job = new Job(hadoopConf);
		job.setJarByClass(VisitPhoneBrandLauncher.class);
		job.setMapperClass(VisitPhoneBrandMapper.class);
		job.setReducerClass(VisitPhoneBrandReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.waitForCompletion(true);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(
				new Configuration(), new VisitPhoneBrandLauncher(), args);
		System.exit(res);
	}

}
