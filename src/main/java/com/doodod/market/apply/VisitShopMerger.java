package com.doodod.market.apply;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.market.statistic.Common;

public class VisitShopMerger extends 
	Mapper<Text, BytesWritable, Text, BytesWritable> {
	
	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		byte[] val = value.getBytes();
		byte[] res = new byte[value.getLength() + 1];
		int index = 0;
		res[index++] = Common.MERGE_TAG_T;
		while (index < res.length) {
			res[index] = val[index - 1];
			index++;
		}
		
		BytesWritable out = new BytesWritable(res);
		context.write(key, out);
	}
}
