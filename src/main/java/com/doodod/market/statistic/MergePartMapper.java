/**
 * @author caoyupeng@doodod.com
 */
package com.doodod.market.statistic;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MergePartMapper extends
		Mapper<LongWritable, BytesWritable, LongWritable, BytesWritable> {
	enum PartCounter {
		PART_MAP_OK,
	}

	@Override
	public void map(LongWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		byte[] val = value.getBytes();
		byte[] res = new byte[value.getLength() + 1];
		int index = 0;
		res[index++] = Common.MERGE_TAG_P;
		while (index < res.length) {
			res[index] = val[index - 1];
			index++;
		}

		context.getCounter(PartCounter.PART_MAP_OK).increment(1);
		BytesWritable out = new BytesWritable(res);
		context.write(key, out);
	}
}
