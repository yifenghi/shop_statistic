package com.doodod.market.apply;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.doodod.market.message.Store.Customer;
import com.doodod.market.message.Store.Visit;
import com.doodod.market.statistic.Common;

public class VisitShopReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
	enum JobCounter {
		TOTAL_REDUCE_OK,
		PART_REDUCE_OK,
		INVALID_RECORD,
		VISIT_MULIT_SHOP,
	}
	@Override
	public void reduce(Text key, Iterable<BytesWritable> values, Context context)
			throws IOException, InterruptedException {
		Customer.Builder total = Customer.newBuilder();
		Customer.Builder part = Customer.newBuilder();
		
		for (BytesWritable val : values) {
			byte[] arr = val.getBytes();
			if (arr[0] == Common.MERGE_TAG_T) {
				total.clear().mergeFrom(val.getBytes(), 1, val.getLength() - 1);
				context.getCounter(JobCounter.TOTAL_REDUCE_OK).increment(1);
			} else if (arr[0] == Common.MERGE_TAG_P) {
				part.clear().mergeFrom(val.getBytes(), 1, val.getLength() - 1);
				context.getCounter(JobCounter.PART_REDUCE_OK).increment(1);
			} else {
				context.getCounter(JobCounter.INVALID_RECORD).increment(1);
			}
		}
		
		if (!total.hasPhoneMac()) {
			total.clear().mergeFrom(part.build());
		}
		else if (part.hasPhoneMac()) {
			Map<Integer, Visit> visitMap = new HashMap<Integer, Visit>();
			for (Visit visit : part.getUserVisitList()) {
				visitMap.put(visit.getShopId(), visit);
			}
			
			Set<Integer> shopVisited = new HashSet<Integer>();
			for (Visit.Builder visit : total.getUserVisitBuilderList()) {
				int shopId = visit.getShopId();
				if (visitMap.containsKey(shopId)) {
					Visit newCustVisit = visitMap.get(shopId);
					for (long date : newCustVisit.getVisitDateList()) {
						visit.addVisitDate(date);
					}
					shopVisited.add(shopId);
				}
			}
			
			Iterator<Integer> iter = visitMap.keySet().iterator();
			while (iter.hasNext()) {
				int shopId = iter.next();
				if (shopVisited.contains(shopId)) {
					continue;
				}
				total.addUserVisit(visitMap.get(shopId));
				context.getCounter(JobCounter.VISIT_MULIT_SHOP).increment(1);
			}
		}
		
		context.write(key, new BytesWritable(total.build().toByteArray()));
	}

}
