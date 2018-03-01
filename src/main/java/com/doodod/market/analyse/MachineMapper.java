package com.doodod.market.analyse;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.market.message.Store.Customer;
import com.doodod.market.message.Store.Shop;
import com.doodod.market.statistic.Common;

public class MachineMapper extends 
	Mapper<LongWritable, BytesWritable, Text, Text> {
	enum JobCounter {
		MACHINE_NUM,
		NOT_MACHINE,
	}
	private static long TIME_FILETER = 0;
	
	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		String timeFilter = context.getConfiguration().get(Common.MACHINE_TIME_FILTER);
		TIME_FILETER = Long.parseLong(timeFilter) * Common.MINUTE_FORMATER * 60;
	}

	@Override
	public void map(LongWritable key, BytesWritable value, Context context)
		throws IOException, InterruptedException {
		DecimalFormat numFormat = new DecimalFormat(Common.NUM_FORNAT);
		Shop.Builder sb = Shop.newBuilder();
		sb.mergeFrom(value.getBytes(), 0, value.getLength());
		
		UserClassifyNew classify = new UserClassifyNew();
		int shopId = sb.getShopId();
		for (Customer cust: sb.getCustomerList()) {
			long timeZone = classify.getMaxTimeClusterZone(cust);
			String phoneMac = new String(cust.getPhoneMac().toByteArray());
			String phoneBrand = new String(cust.getPhoneBrand().toByteArray());
			String rssiVar = numFormat.format(getRssiVar(cust.getApRssiList()));
			
			if (timeZone > TIME_FILETER) {
				context.write(
						new Text(String.valueOf(shopId) + Common.CTRL_A + phoneMac), 
						new Text(phoneBrand + Common.CTRL_A + rssiVar));
				context.getCounter(JobCounter.MACHINE_NUM).increment(1);
			}
			else {
				context.getCounter(JobCounter.NOT_MACHINE).increment(1);
			}
		}
	}
	
	public static float getRssiVar(List<Integer> list) {
		if (list.size() == 0) {
			return 0;
		}
		
		int sum = 0;
		for (int rssi : list) {
			sum += rssi;
		}
		float mean = sum / list.size();

		float var = 0;
		for (int rssi : list) {
			var += Math.pow(rssi - mean, 2);
		}

		var /= list.size();
		return var;
	}
}
