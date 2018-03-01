package com.doodod.market.apply;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.market.statistic.Common;

public class FlowMinMapper extends 
	Mapper<LongWritable, BytesWritable, LongWritable, BytesWritable>{

	@Override
	public void map(LongWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		byte[] arr = value.getBytes();
		byte[] res = new byte[value.getLength() + 1];
		
		res[0] = Common.FLOW_TAG_M;
		for (int i = 1; i < value.getLength(); i++) {
			res[i] = arr[i - 1];
		}
		
		BytesWritable output = new BytesWritable(res);
		context.write(key, output);
	}
	
}
