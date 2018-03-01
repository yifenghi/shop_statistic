package com.doodod.market.apply;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MergeHistoryAndSetVisitMerger extends
		Mapper<Text, BytesWritable, Text, BytesWritable> {

	@Override
	protected void map(Text key, BytesWritable value,Context context)
			throws IOException, InterruptedException {
		context.write(key, value);
	}

}
