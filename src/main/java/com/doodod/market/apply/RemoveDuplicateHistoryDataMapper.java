package com.doodod.market.apply;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.market.message.Store.Customer;
import com.doodod.market.message.Store.Visit;

public class RemoveDuplicateHistoryDataMapper extends
		Mapper<Text, BytesWritable, Text, BytesWritable> {
	enum JobCounter {
		DATE_FORMAT_ERROR,
		DATE_ADD_OK,
		DATE_DUPLICATE,
		MAP_OK
	}
	@Override
	protected void map(Text key, BytesWritable value,Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(),0, value.getLength());
		Set<Long> dateSet = new TreeSet<Long>();
		for(Visit.Builder vb:cb.getUserVisitBuilderList()){
			
			dateSet.clear();
			for(long date:vb.getVisitDateList()){				
				
				try {
					
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd 00:00:00");
					
					date=sdf.parse(sdf.format(date)).getTime();
					
				} catch (ParseException e) {
					
					context.getCounter(JobCounter.DATE_FORMAT_ERROR).increment(1);
				}
				
				if(dateSet.add(date)){
					
					context.getCounter(JobCounter.DATE_ADD_OK).increment(1);
					
				}else{
					context.getCounter(JobCounter.DATE_DUPLICATE).increment(1);
				}
				
			}
			
			vb.clearVisitDate();
			vb.addAllVisitDate(dateSet);			
		}
		context.getCounter(JobCounter.MAP_OK).increment(1);
		context.write(key, new BytesWritable(cb.build().toByteArray()));
		
	}

}
