package com.doodod.market.analyse;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.market.message.Store.Customer;
import com.doodod.market.message.Store.Shop;
import com.doodod.market.message.Store.UserType;
import com.doodod.market.statistic.Common;

public class EmployeeMapper extends 
	Mapper<LongWritable, BytesWritable, Text, Text> {
	enum JobCounter {
		EMPLOYEE_NUM,
		NOT_EMPLOYEE
	}
	
	private static long TIME_FILETER = 1 * Common.MINUTE_FORMATER * 60;
	
	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		int timeFilter = Integer.parseInt(
				context.getConfiguration().get(Common.EMPLOYEE_TIME_FILTER));
		TIME_FILETER = timeFilter * Common.MINUTE_FORMATER * 60;
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
			if (cust.getUserType() == UserType.MACHINE 
					|| cust.getUserType() == UserType.PASSENGER) {
				continue;
			}
			
			long timeZone = classify.getTotalTimeClusterZone(cust);
			String phoneMac = new String(cust.getPhoneMac().toByteArray());
			String phoneBrand = new String(cust.getPhoneBrand().toByteArray());
			String rssiVar = numFormat.format(MachineMapper.getRssiVar(cust.getApRssiList()));
			
			if (timeZone > TIME_FILETER) {
				context.write(
						new Text(String.valueOf(shopId) + Common.CTRL_A + phoneMac), 
						new Text(phoneBrand + Common.CTRL_A + rssiVar));
				context.getCounter(JobCounter.EMPLOYEE_NUM).increment(1);
			}
			else {
				context.getCounter(JobCounter.NOT_EMPLOYEE).increment(1);
			}
		}
	}
	

}
