package com.doodod.market.analyse;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class ClassifyTrainMapper extends 
	Mapper<LongWritable, BytesWritable, LongWritable, BytesWritable> {

	@Override
	public void map(LongWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		context.write(key, value);
	}
	

}
