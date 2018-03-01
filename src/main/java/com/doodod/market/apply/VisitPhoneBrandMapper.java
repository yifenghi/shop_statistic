package com.doodod.market.apply;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.market.message.Store.Customer;
import com.doodod.market.message.Store.Shop;
import com.doodod.market.statistic.Common;

public class VisitPhoneBrandMapper extends
		Mapper<LongWritable, BytesWritable, Text, LongWritable> {
	enum JobCounter {
		NO_PHONE_BRAND, BRAND_UNKOWN, BRAND_OTHER,
	}

	private static Set<String> PHONE_BRAND_SET = new HashSet<String>();

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
	/*	Charset charSet = Charset.forName("UTF-8");
		String brandListPath = context.getConfiguration().get(
				Common.CONF_BRAND_LIST);
		BufferedReader brandReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(brandListPath), charSet));
		String line = "";
		while ((line = brandReader.readLine()) != null) {
			String arr[] = line.split(Common.CTRL_A, -1);
			if (arr.length < 2) {
				continue;
			}
			PHONE_BRAND_SET.add(arr[1]);
		}
		brandReader.close();*/
	}

	@Override
	public void map(LongWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Shop.Builder sb = Shop.newBuilder();
		sb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		for (Customer cb : sb.getCustomerList()) {

			if (!cb.hasPhoneBrand()) {
				context.getCounter(JobCounter.NO_PHONE_BRAND).increment(1);
				return;
			}

			String phoneBrand = new String(cb.getPhoneBrand().toByteArray());
			if (phoneBrand.equals(Common.BRAND_NAME_UNKNOWN)) {
				context.getCounter(JobCounter.BRAND_UNKOWN).increment(1);
				phoneBrand = Common.DEFAULT_MAC_BRAND;
			} else {
				//context.getCounter(JobCounter.BRAND_OTHER).increment(1);
				phoneBrand = new String(cb.getPhoneBrand().toByteArray());
			}

			context.write(new Text(phoneBrand), new LongWritable(1));

		}
	}

}
