package com.doodod.market.analyse;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;

import com.doodod.market.message.Store.Customer;
import com.doodod.market.message.Store.Shop;
import com.doodod.market.message.Store.UserType;

public class Common {
	public static SequenceFile.Reader getSeqReader(String inputDir, Configuration conf) 
			throws IOException {
		FileSystem in = FileSystem.get(URI.create(inputDir), conf);
		Path pathIn = new Path(inputDir);
		@SuppressWarnings("deprecation")
		SequenceFile.Reader reader = new SequenceFile.Reader(in, pathIn, conf);
		return reader;
	}
	
	public static List<Shop> getShopList(String inputDir) throws IOException {
		Configuration seqConf = new Configuration();
		SequenceFile.Reader reader = 
				getSeqReader(inputDir, seqConf);	
		List<Shop> res = new ArrayList<Shop>();

		Object inKey = null;
		Object inVal = null;
		
		inKey = reader.next(inKey);
		while (null != inKey) {
			inVal = reader.getCurrentValue(inVal);
			BytesWritable bytesObj = (BytesWritable) inVal;
			
			Shop.Builder sb = Shop.newBuilder();
			sb.clear().mergeFrom(bytesObj.getBytes(), 0, bytesObj.getLength());
			res.add(sb.build());
			inKey = reader.next(inKey);
		}
		reader.close();
		
		return res;
	}
	
	public static void main(String[] args) throws IOException {
		String inputDir = "/Users/paul/Documents/code_dir/doodod/shopstatistic/data/data.7.14";

		SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		int counter = 0;
		
		Shop shop = getShopList(inputDir).get(1);
		for (int i = 65; i < shop.getCustomerCount(); i++) {
			Customer cust = shop.getCustomer(i);
			if (cust.getUserType() != UserType.PASSENGER) {
				List<Long> timeList = cust.getTimeStampList();
				if (timeList.size() > 10) {
					continue;
				}
				
				List<Long> timeListSort = new ArrayList<Long>(timeList);
				//Collections.sort(timeListSort);
				
				for (long time : timeListSort) {
					System.out.println(timeFormat.format(new Date(time)));
				}
				System.out.println("END");
				counter ++;
				
				if (counter == 30) {
					System.out.println(i);
					break;
				}
			}
		}
		
	}

}
