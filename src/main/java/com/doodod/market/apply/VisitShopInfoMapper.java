package com.doodod.market.apply;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.market.message.Store.Customer;
import com.doodod.market.message.Store.UserType;
import com.doodod.market.message.Store.Visit;
import com.doodod.market.statistic.Common;

public class VisitShopInfoMapper extends 
	Mapper<Text, BytesWritable, LongWritable, Text> {
	private static long DATE_TIME = 0;

	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		try {
			DATE_TIME = timeFormat.parse(
					context.getConfiguration().get(Common.BIZDATE))
					.getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		if (cb.hasUserType() && 
				cb.getUserType() == UserType.CUSTOMER) {
			for (Visit visit : cb.getUserVisitList()) {
				int times = visit.getVisitDateCount();
				
				if (DATE_TIME != visit.getVisitDate(times - 1)) {
					continue;
				}
				
				int newCustNum = 0;
				int oldCustNum = 0;
				if (times > 1) {
					newCustNum ++;
				}
				else {
					oldCustNum ++;
				}
				
				LongWritable outKey = new LongWritable(visit.getShopId());
				Text outVal = new Text(newCustNum + Common.CTRL_A + oldCustNum);
				
				context.write(outKey, outVal);
			}
		}
		

	}
}
