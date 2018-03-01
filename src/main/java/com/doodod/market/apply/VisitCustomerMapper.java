package com.doodod.market.apply;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.market.message.Store.Customer;
import com.doodod.market.message.Store.UserType;
import com.doodod.market.message.Store.Visit;
import com.doodod.market.statistic.Common;

public class VisitCustomerMapper extends
	Mapper<Text, BytesWritable, LongWritable, Text> {
	private static int DEFAULT_SHOPID = 0;
	
	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		
		int customerNum = 0;
		int employeeNum = 0;
		int machineNum = 0;
		int passengerNum = 0;
		
		if (!cb.hasUserType()) {
			customerNum ++;
		}
		else {
			switch (cb.getUserType().getNumber()) {
			case UserType.CUSTOMER_VALUE:
				customerNum ++;
				break;
			case UserType.EMPLOYEE_VALUE:
				employeeNum ++;
				break;
			case UserType.MACHINE_VALUE:
				machineNum ++;
				break;
			case UserType.PASSENGER_VALUE:
				passengerNum ++;
				break;
			default:
				break;
			}
		}
		
		Text outVal = new Text(customerNum + Common.CTRL_A
				+ employeeNum + Common.CTRL_A
				+ machineNum + Common.CTRL_A
				+ passengerNum + Common.CTRL_A
				+ 1);
		
		for (Visit visit : cb.getUserVisitList()) {
			int shopId = visit.getShopId();
			context.write(new LongWritable(shopId), outVal);
		}
		
		context.write(new LongWritable(DEFAULT_SHOPID), outVal);	
	}

}
