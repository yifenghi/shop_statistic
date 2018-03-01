package com.doodod.market.apply;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.market.message.Store.Customer;
import com.doodod.market.message.Store.Visit;
import com.doodod.market.statistic.Common;
import com.google.protobuf.ByteString;



public class MergeHistoryAndSetVisitMapper extends
		Mapper<Text, Text, Text, BytesWritable> {

	
	enum JobCounter {
		MAP_ERROR_DATA,
		BRAND_MAP_ERROR,
		READ_BRAND_OK
	}
	private static Map<String, String> macBrandMap = new HashMap<String, String>();
	
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		
		String brandPath = context.getConfiguration().get(Common.BRAND_MAP_PATH);
		Charset charSet =  Charset.forName("UTF-8");
		BufferedReader brandReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(brandPath), charSet));
		String line = "";
		while ((line = brandReader.readLine()) != null) {
			String[] arrBrand = line.split(Common.CTRL_A, -1);
			if (arrBrand.length != 2) {
				context.getCounter(JobCounter.BRAND_MAP_ERROR).increment(1);		
				continue;
			}
			macBrandMap.put(arrBrand[0], arrBrand[1]);
			context.getCounter(JobCounter.READ_BRAND_OK).increment(1);
		}
		brandReader.close();

		
	}


	@Override
	protected void map(Text key, Text value,Context context)
			throws IOException, InterruptedException {

		Customer.Builder cb = Customer.newBuilder();
		Visit.Builder vb = Visit.newBuilder();
		String res[]=value.toString().split(Common.CTRL_A, -1);
		if(res.length!=2){
			context.getCounter(JobCounter.MAP_ERROR_DATA).increment(1);
			return;
		}
		String timeStamp=res[0];
		String shopId=res[1];
		
		vb.setShopId(Integer.parseInt(shopId));
		vb.addVisitDate(Long.parseLong(timeStamp));
		cb.addUserVisit(vb);
		String phoneBrand = Common.BRAND_NAME_UNKNOWN;
		String preMac=key.toString().substring(0, 8);
		if (macBrandMap.containsKey(preMac)) {
			phoneBrand = macBrandMap.get(preMac);
		}
		cb.setPhoneBrand(ByteString.copyFrom(phoneBrand.getBytes()));
		context.write(key, new BytesWritable(cb.build().toByteArray()));
		
		
	}

}
