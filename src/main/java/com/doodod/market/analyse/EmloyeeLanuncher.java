package com.doodod.market.analyse;

import java.text.SimpleDateFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.doodod.market.statistic.Common;

public class EmloyeeLanuncher extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		GenericOptionsParser options = new GenericOptionsParser(getConf(), args);
		Configuration hadoopConf = options.getConfiguration();
		
		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		long timeUp = timeFormat.parse(
				hadoopConf.get(Common.BUSINESSTIME_NOW)).getTime();
		hadoopConf.set(Common.BUSINESSTIME_NOW, String.valueOf(timeUp));
		
		Job job = new Job(hadoopConf);
		job.setJarByClass(EmloyeeLanuncher.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(EmployeeMapper.class);
		job.setReducerClass(EmployeeReducer.class);
		
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(
				ToolRunner.run(new Configuration(), new EmloyeeLanuncher(), args));
	}

}
