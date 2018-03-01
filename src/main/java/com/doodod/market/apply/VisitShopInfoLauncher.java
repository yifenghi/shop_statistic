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

public class VisitShopInfoLauncher extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(
				new Configuration(), new VisitShopInfoLauncher(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		GenericOptionsParser options = new GenericOptionsParser(getConf(), args);
		Configuration hadoopConf = options.getConfiguration();
		
		Job job = new Job(hadoopConf);
		job.setJarByClass(VisitShopInfoLauncher.class);
		job.setMapperClass(VisitShopInfoMapper.class);
		job.setReducerClass(VisitShopInfoReducer.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
		
		return 0;
	}

}
