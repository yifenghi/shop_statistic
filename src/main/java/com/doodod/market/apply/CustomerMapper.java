package com.doodod.market.apply;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.market.analyse.UserClassifyNew;
import com.doodod.market.message.Store.Customer;
import com.doodod.market.message.Store.Shop;
import com.doodod.market.statistic.Common;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class CustomerMapper extends 
	Mapper<LongWritable, BytesWritable, Text, BytesWritable>{
	enum JobCounter {
		SHOP_OK,
		CUSTOMER_OK,
		DB_WRITE_OK,
	}
	
	private static String KEY_SHOPID;
	private static String KEY_CREATETIME;
	private static String KEY_UPDATETIME;
	private static String KEY_USERMAC;
	private static String KEY_DWELL;
	private static String KEY_TIMESTAMPS;
	private static String KEY_USERTYPE;

	
	private static long UPDATE_TIME;
	private static long CREATE_TIME;
	
	private static Map<String, DBObject> custDwellMap = new HashMap<String, DBObject>(); 
	@Override
	public void setup(Context context) throws IOException,
			InterruptedException {
		KEY_SHOPID = context.getConfiguration().get(
				Common.MONGO_COLLECTION_CUSTOMER_SHOPID);
		KEY_CREATETIME = context.getConfiguration().get(
				Common.MONGO_COLLECTION_CUSTOMER_CREATE);
		KEY_UPDATETIME = context.getConfiguration().get(
				Common.MONGO_COLLECTION_CUSTOMER_UPDATE);
		KEY_USERMAC = context.getConfiguration().get(
				Common.MONGO_COLLECTION_CUSTOMER_USERMAC);
		KEY_DWELL = context.getConfiguration().get(
				Common.MONGO_COLLECTION_CUSTOMER_DWELL);
		KEY_TIMESTAMPS = context.getConfiguration().get(
				Common.MONGO_COLLECTION_CUSTOMER_TIMESATMPS);
		KEY_USERTYPE = context.getConfiguration().get(
				Common.MONGO_COLLECTION_CUSTOMER_TYPE);
		
		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		try {
			UPDATE_TIME = timeFormat.parse(context.getConfiguration().get(
					Common.CUSTOMER_TIME_UPDATE)).getTime();
			CREATE_TIME = timeFormat.parse(context.getConfiguration().get(
					Common.CUSTOMER_TIME_CREATE)).getTime();
			CREATE_TIME --;
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	
	}

	@Override
	public void map(LongWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Shop.Builder sb = Shop.newBuilder();
		sb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		context.getCounter(JobCounter.SHOP_OK).increment(1);
		
		UserClassifyNew classify = new UserClassifyNew();
		int shopId = sb.getShopId();
		for (Customer cust : sb.getCustomerList()) {
			long dwell = classify.getTotalTimeClusterZone(cust);
			
			DBObject obj = new BasicDBObject();
			String phoneMac = new String(cust.getPhoneMac().toByteArray());
			obj.put(KEY_USERMAC, phoneMac);
			//store seconds in mongo
			obj.put(KEY_DWELL, (int)(dwell / Common.MINUTE_FORMATER * 60 ));
			obj.put(KEY_SHOPID, String.valueOf(shopId));
			obj.put(KEY_TIMESTAMPS, cust.getTimeStampList());
			//(user_type:int) CUSTOMER:0, PASSENGER:1, EMPLOYEE:2, MACHINE:3
			obj.put(KEY_USERTYPE, cust.getUserType().getNumber());
						
			context.getCounter(JobCounter.CUSTOMER_OK).increment(1);
			
			//the data will update when a customer enter different shop
			custDwellMap.put(phoneMac, obj);
		}
	}

	@Override
	public void cleanup(Context context)
			throws IOException, InterruptedException {
		String mongoServerList = context.getConfiguration().get(
				Common.MONGO_SERVER_LIST);
		String custCollectionName = context.getConfiguration().get(
				Common.MONGO_COLLECTION_CUSTOMERINFO);
		String serverArr[] = mongoServerList.split(Common.COMMA, -1);
		if (serverArr.length != Common.MONGO_SERVER_NUM) {
			throw new RuntimeException("Get mongo server fail.");
		}
		String mongoServerFst = serverArr[0];
		String mongoServerSnd = serverArr[1];
		String mongoServerTrd = serverArr[2];

		String mongoDbName = context.getConfiguration().get(
				Common.MONGO_DB_NAME);
		int mongoServerPort = Integer.parseInt(context.getConfiguration().get(
				Common.MONGO_SERVER_PORT));

		MongoClient mongoClient = new MongoClient(Arrays.asList(
				new ServerAddress(mongoServerFst, mongoServerPort),
				new ServerAddress(mongoServerSnd, mongoServerPort),
				new ServerAddress(mongoServerTrd, mongoServerPort)));
		DB mongoDb = mongoClient.getDB(mongoDbName);
		DBCollection custCollection = 
				mongoDb.getCollection(custCollectionName);
		
		Iterator<String> iter = custDwellMap.keySet().iterator();
		while (iter.hasNext()) {
			String phoneMac = iter.next();
            DBObject obj = custDwellMap.get(phoneMac);	
			obj.put(KEY_CREATETIME, CREATE_TIME);
			obj.put(KEY_UPDATETIME, UPDATE_TIME);
			
            BasicDBObject query = new BasicDBObject();
            query.append(KEY_USERMAC, phoneMac);            
            query.append(KEY_SHOPID, obj.get(KEY_SHOPID));
            query.append(KEY_UPDATETIME, UPDATE_TIME);
            
            custCollection.update(query, obj, true, false);
            context.getCounter(JobCounter.DB_WRITE_OK).increment(1);
		}
		
		mongoClient.close();
		
	}
	
	
}
