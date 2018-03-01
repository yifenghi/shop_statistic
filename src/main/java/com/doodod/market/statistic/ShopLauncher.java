/**
 * @author caoyupeng@doodod.com
 */
package com.doodod.market.statistic;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ShopLauncher extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		GenericOptionsParser options = new GenericOptionsParser(getConf(), args);
		Configuration hadoopConf = options.getConfiguration();

		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		long timeStart = timeFormat.parse(
				hadoopConf.get(Common.BUSINESSTIME_START)).getTime();
		long timeEnd = timeFormat.parse(
				hadoopConf.get(Common.BUSINESSTIME_NOW)).getTime();
		SimpleDateFormat dataFormat = new SimpleDateFormat(Common.DATE_FORMAT);
		String date = dataFormat.format(new Date(timeEnd));

		hadoopConf.set(Common.BIZDATE, date);
		Job job = new Job(hadoopConf);

		Scan scan = new Scan();
		scan.setCaching(3000);
		scan.setCacheBlocks(false);
		scan.setTimeRange(timeStart, timeEnd);
		scan.setMaxVersions();
		scan.addColumn(Bytes.toBytes(Common.HBASE_RSSI),
				Bytes.toBytes(Common.HBASE_RSSI));
		TableMapReduceUtil.initTableMapperJob(Common.HBASE_RSSI, scan,
				ShopMapper.class, LongWritable.class, BytesWritable.class, job);

		MultipleOutputs.addNamedOutput(job, Common.BUSINESSTIME_OUT,
				SequenceFileOutputFormat.class, LongWritable.class, BytesWritable.class);
		job.setReducerClass(ShopReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new ShopLauncher(), args));
	}

}
