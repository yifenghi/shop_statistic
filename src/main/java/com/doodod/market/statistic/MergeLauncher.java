/**
 * @author caoyupeng@doodod.com
 */
package com.doodod.market.statistic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MergeLauncher extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		GenericOptionsParser options = new GenericOptionsParser(getConf(), args);
		// String[] otherArgs = options.getRemainingArgs();
		Configuration hadoopConf = options.getConfiguration();

		Job job = new Job(hadoopConf);
		// why this can solve problem
		job.setJarByClass(MergeLauncher.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setReducerClass(MergeReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		MultipleInputs.addInputPath(job,
				new Path(hadoopConf.get(Common.MERGE_INPUT_PART)),
				SequenceFileInputFormat.class, MergePartMapper.class);
		MultipleInputs.addInputPath(job,
				new Path(hadoopConf.get(Common.MERGE_INPUT_TOTAL)),
				SequenceFileInputFormat.class, MergeTotalMapper.class);
		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new MergeLauncher(),
				args));
	}

}
