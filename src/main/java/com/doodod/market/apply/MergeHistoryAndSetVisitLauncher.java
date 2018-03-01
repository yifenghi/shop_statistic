package com.doodod.market.apply;
//add by lifeng
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.doodod.market.statistic.*;

public class MergeHistoryAndSetVisitLauncher extends Configured implements Tool {

	public int run(String[] arg0) throws Exception {
		
		GenericOptionsParser options=new GenericOptionsParser(getConf(),arg0);
		Configuration hadoopConf = options.getConfiguration();
		
		Job job= new Job(hadoopConf);
		
		job.setJarByClass(MergeHistoryAndSetVisitLauncher.class);
		

//
//		MultipleInputs.addInputPath(job,
//				new Path(hadoopConf.get(Common.MERGE_INPUT_PART)),
//				SequenceFileInputFormat.class, MergeHistoryAndSetVisitMapper.class);
		
		
//		MultipleInputs.addInputPath(job,
//				new Path(hadoopConf.get(Common.MERGE_INPUT_TOTAL)),
//				SequenceFileInputFormat.class, MergeHistoryAndSetVisitMerger.class);
		
		
				
		job.setMapperClass(MergeHistoryAndSetVisitMerger.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);		
		job.setReducerClass(MergeHistoryAndSetVisitReducer.class);
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

		System.exit(ToolRunner.run(new Configuration(), new MergeHistoryAndSetVisitLauncher(), args));

	}

}
