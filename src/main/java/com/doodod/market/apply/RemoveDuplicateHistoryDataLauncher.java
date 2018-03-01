package com.doodod.market.apply;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RemoveDuplicateHistoryDataLauncher extends Configured implements Tool {

	public int run(String[] arg0) throws Exception {
		GenericOptionsParser options=new GenericOptionsParser(getConf(),arg0);
		Configuration hadoopConf = options.getConfiguration();
		
		Job job= new Job(hadoopConf);
		
		job.setJarByClass(RemoveDuplicateHistoryDataLauncher.class);			
		job.setMapperClass(RemoveDuplicateHistoryDataMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);		
		job.waitForCompletion(true);
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new RemoveDuplicateHistoryDataLauncher(), args));

	}

}
