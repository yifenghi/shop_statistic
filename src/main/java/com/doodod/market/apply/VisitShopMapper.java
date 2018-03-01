package com.doodod.market.apply;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.market.message.Store.Customer;
import com.doodod.market.message.Store.Shop;
import com.doodod.market.message.Store.Visit;
import com.doodod.market.statistic.Common;

public class VisitShopMapper extends 
	Mapper<LongWritable, BytesWritable, Text, BytesWritable> {
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
	public void map(LongWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Shop.Builder sb = Shop.newBuilder();
		sb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		int shopId = sb.getShopId();
		
		for (Customer.Builder customer : sb.getCustomerBuilderList()) {
			customer.clearApRssi().clearTimeStamp();
			Visit.Builder vb = Visit.newBuilder();
			vb.setShopId(shopId);
			vb.addVisitDate(DATE_TIME);
			customer.addUserVisit(vb.build());
			
			String custMac = new String(customer.getPhoneMac().toByteArray());			
			byte[] val = customer.build().toByteArray();
			byte[] res = new byte[val.length + 1];
			int index = 0;
			res[index ++] = Common.MERGE_TAG_P;
			while (index < res.length) {
				res[index] = val[index - 1];
				index ++ ;
			}
			
			context.write(new Text(custMac), new BytesWritable(res));
		}
	}
}
