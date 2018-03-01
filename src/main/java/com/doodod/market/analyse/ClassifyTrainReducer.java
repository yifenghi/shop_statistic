package com.doodod.market.analyse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.doodod.market.message.Store.Customer;
import com.doodod.market.message.Store.Shop;
import com.doodod.market.statistic.Common;

public class ClassifyTrainReducer extends 
	Reducer<LongWritable, BytesWritable, LongWritable, Text> {
	enum JobCounter {
		TIME_STAMP_MIN,
		MODEL_SHOP_COUNT,
	}
	List<Integer> passFeatureList = new ArrayList<Integer>();

	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		String list = context.getConfiguration().get(Common.TRAIN_PASSENGER_FEATURELIST);
		String arr[] = list.split(Common.COMMA, -1);
		
		for (String str : arr) {
			passFeatureList.add(Integer.parseInt(str));
		}

		String strIndoor = context.getConfiguration().get(Common.RSSI_INDOOR_SCOPE);
		UserClassifyNew.Rssi_Indoor = Integer.parseInt(strIndoor);
		
	}

	@Override
	public void reduce(LongWritable key, Iterable<BytesWritable> values, Context context)
			throws IOException, InterruptedException {
		Shop.Builder sb = Shop.newBuilder();
		//merge shop info together, do not merge customer 
		for (BytesWritable val : values) {
			byte[] arr = val.getBytes();
			sb.mergeFrom(arr, 0, val.getLength());
		}

		UserClassifyNew classify = new UserClassifyNew();		
		for (Customer.Builder cb : sb.getCustomerBuilderList()) {
			classify.PreProcess(cb, passFeatureList);
		}
		context.getCounter(JobCounter.MODEL_SHOP_COUNT).increment(1);
		classify.trainPassenger(sb.build(), passFeatureList);
		
		Text output = new Text(classify.toString());		
		context.write(key, output);
	}
}
