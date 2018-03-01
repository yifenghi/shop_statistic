package com.doodod.market.apply;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.doodod.market.statistic.Common;
import com.doodod.market.statistic.ShopLauncher;
import com.doodod.market.statistic.ShopMapper;
import com.doodod.market.statistic.ShopReducer;

public class CalculateVisitForHistoryLauncher extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		GenericOptionsParser options = new GenericOptionsParser(getConf(), args);
		Configuration hadoopConf = options.getConfiguration();
		
		Job job = new Job(hadoopConf);
		
		
		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		long timeStart = timeFormat.parse(
				hadoopConf.get(Common.BUSINESSTIME_START)).getTime();
		long timeEnd = timeFormat.parse(
				hadoopConf.get(Common.BUSINESSTIME_END)).getTime();
//		long timeStart = timeFormat.parse("2014-11-20 19:00:00").getTime();
//		long timeEnd = timeFormat.parse("2014-11-20 19:05:00").getTime();		
		
		
		
		
		Scan scan = new Scan();
		scan.setCaching(3000);
		scan.setCacheBlocks(false);
		scan.setTimeRange(timeStart, timeEnd);
		scan.setMaxVersions();
		scan.addColumn(Bytes.toBytes(Common.HBASE_RSSI),
				Bytes.toBytes(Common.HBASE_RSSI));
		TableMapReduceUtil.initTableMapperJob(Common.HBASE_RSSI, scan,
				CalculateVisitForHistoryMapper.class, Text.class, Text.class, job);
		job.setJarByClass(CalculateVisitForHistoryLauncher.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.waitForCompletion(true);
		
		return 0;
	}

	/**
	 * @param args
	 */
	public static void main (String[] args) throws Exception{
		System.exit(ToolRunner.run(new Configuration(), new CalculateVisitForHistoryLauncher(), args));

	}

}
