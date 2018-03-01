package com.doodod.market.apply;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import com.doodod.market.message.Store.Customer;
import com.doodod.market.message.Store.UserType;
import com.doodod.market.message.Store.Visit;
import com.doodod.market.statistic.Common;
import com.google.protobuf.ByteString;

public class MergeHistoryAndSetVisitReducer extends
		Reducer<Text, BytesWritable, Text, BytesWritable> {
	
	
	enum JobCounter {
		REDUCE_ERROR_DATA
	}
	@Override
	protected void reduce(Text key, Iterable<BytesWritable> values,Context context)
			throws IOException, InterruptedException {
		
		
		Customer.Builder cb = Customer.newBuilder();
		HashMap<Integer,  ArrayList<Long>> visitMap = new HashMap<Integer, ArrayList<Long>>();
		ArrayList<Long> temp = new ArrayList<Long>();
		UserType userType=null;
		ByteString phoneBrand = ByteString.EMPTY;
		for(BytesWritable val:values){
			
			cb.clear().mergeFrom(val.getBytes(),  0, val.getLength());
			if(cb.hasUserType()){
				userType=cb.getUserType();
			}
			if(cb.hasPhoneBrand()){
				phoneBrand=cb.getPhoneBrand();
			}
			for (Visit.Builder visit : cb.getUserVisitBuilderList()) {
				
				int shopId = visit.getShopId();
				
				if (visitMap.containsKey(shopId)) {
					
					for(int i=0;i<visit.getVisitDateList().size();i++){
						
						visitMap.get(shopId).add(visit.getVisitDate(i));
					}
					
				}else{
					
					temp.clear();
					for(Long l:visit.getVisitDateList()){
						temp.add(l);
					}
					visitMap.put(shopId, temp);
				}	
				
			}	
			
			
		}
		
		cb.clear();
		Iterator<Integer> iter = visitMap.keySet().iterator();
		Visit.Builder vb= Visit.newBuilder();
		ArrayList<Long> ary=new ArrayList<Long>();
		while (iter.hasNext()) {
			vb.clear();
			int shopId=iter.next();
			ary.clear();
			ary.addAll(visitMap.get(shopId));
			Collections.sort(ary);
			visitMap.get(shopId).clear();
			visitMap.get(shopId).addAll(ary);
			
			vb.setShopId(shopId);
			vb.addAllVisitDate(visitMap.get(shopId));
			cb.addUserVisit(vb);
		}	
		
		if(userType!=null){
			cb.setUserType(userType);			
		}
		if(!phoneBrand.isEmpty()){
			cb.setPhoneBrand(phoneBrand);
		}
		
		context.write(key, new BytesWritable(cb.build().toByteArray()));
		
		
		
		
		
		
		
		
		
	}

}
