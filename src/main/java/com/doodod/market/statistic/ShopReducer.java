/**
 * @author caoyupeng@doodod.com
 */
package com.doodod.market.statistic;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.doodod.market.analyse.UserClassifyNew;
import com.doodod.market.message.Store.Customer;
import com.doodod.market.message.Store.Shop;
import com.doodod.market.message.Store.UserType;
import com.doodod.market.statistic.UserClassifyFunc;
import com.doodod.market.statistic.MergeReducer.JobCounter;
import com.google.protobuf.ByteString;


public class ShopReducer extends
		Reducer<LongWritable, BytesWritable, LongWritable, BytesWritable> {
	enum JobCounter {
		SHOP_OK, 
		CUST_NUM_BEFORE, 
		CUST_NUM_UNIQ,
		CUST_NUM_SHOP,
		CUST_MACHINE, 
		CUST_EMPLOYEE, 
		CUST_PASSENGER, 
		NEWCUST_NUM_ERROR, 
		EMPLOYEE_MAP_OK, 
		MACHINE_MAP_OK,
		DB_SHOPID_NULL,
		DB_USERTYPE_NULL,
		DB_PHONEMAC_NULL,
		TIME_RSSI_NOT_EQUAL,
		AP_TOTAL_NUM,
		AP_FAIL_NUM,
		DB_TIMESHOPID_NULL,
		DB_TIMESTART_NULL,
		DB_TIMEEND_NULL,
		OUT_OF_TIME_RANGE,
		MODEL_TABLE_OFF,
		MODEL_TABLE_ERROR,
		SHOP_WHITHOUT_BUSINESSTIME,
		STRONG_RSSI
	}
	
	private static int RSSI_FILTER = 0;
	private static String DATE = null;
	private static String TIME_START = null;
	private static int DEFAULT_FILTER = 0;
	private static Map<Integer, Set<String>> machineMap = new HashMap<Integer, Set<String>>();
	private static Map<Integer, Set<String>> employeeMap = new HashMap<Integer, Set<String>>();
	private static Map<Integer, List<Long>> shopBusinessTimeMap = new HashMap<Integer, List<Long>>();
	private static Map<Integer, UserClassifyNew> shopModelMap = new HashMap<Integer, UserClassifyNew>();
	private static List<Integer> featureList = new ArrayList<Integer>();

	//private static Map<String, Boolean> apStatusMap = new HashMap<String, Boolean>();
	private static Set<String> apOKSet = new HashSet<String>();
	private static MultipleOutputs<LongWritable, BytesWritable> multipleOutputs;

	@Override
	public void setup(Context context) throws IOException,
			InterruptedException {
		RSSI_FILTER = context.getConfiguration().getInt(
				Common.RSSI_INDOOR_SCOPE, Common.DEFAULT_INDOOR_SCOPE);
		
		DATE = context.getConfiguration().get(Common.BIZDATE);
		TIME_START = context.getConfiguration().get(Common.BUSINESSTIME_START);
		DEFAULT_FILTER = context.getConfiguration().getInt(
				Common.TRAIN_PASSENGER_DEFAULT_FILTER, Common.DEFAULT_PASSENGER_FILTER);
		
		String mongoServerList = context.getConfiguration().get(
				Common.MONGO_SERVER_LIST);
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

		String typeCollectionName = context.getConfiguration().get(
				Common.MONGO_COLLECTION_USERINFO);
		String keyShopId = context.getConfiguration().get(
				Common.MONGO_COLLECTION_USER_SHOPID);
		String keyType = context.getConfiguration().get(
				Common.MONGO_COLLECTION_USER_TYPE);
		String keyMac = context.getConfiguration().get(
				Common.MONGO_COLLECTION_USER_MAC);
		
		String timeCollectionName = context.getConfiguration().get(
				Common.MONGO_COLLECTION_BUSINESSTIME);
		String timeCollectionKeyStartTime = context.getConfiguration().get(
				Common.MONGO_COLLECTION_BUSINESSTIME_STRATTIME);
		String timeCollectionKeyEndTime = context.getConfiguration().get(
				Common.MONGO_COLLECTION_BUSINESSTIME_ENDTIME);
		String timeCollectionKeyShopId = context.getConfiguration().get(
				Common.MONGO_COLLECTION_BUSINESSTIME_SHOPID);

		DB mongoDb = mongoClient.getDB(mongoDbName);
		DBCollection typeCollection = mongoDb.getCollection(typeCollectionName);
		BasicDBObject macRef = new BasicDBObject();
		BasicDBObject macKey = new BasicDBObject();
		macKey.append(keyShopId, 1);
		macKey.append(keyType, 1);
		macKey.append(keyMac, 1);

		DBCursor typeCursor = typeCollection.find(macRef, macKey);
		while (typeCursor.hasNext()) {
			DBObject obj = typeCursor.next();
			
			Object shopIdObj = obj.get(keyShopId);
			if (shopIdObj == null) {
				context.getCounter(JobCounter.DB_SHOPID_NULL).increment(1);
				continue;
			}
			int shopId = Integer.parseInt(shopIdObj.toString());
			
			Object userTypeObj = obj.get(keyType);
			if (userTypeObj == null) {
				context.getCounter(JobCounter.DB_USERTYPE_NULL).increment(1);
				continue;
			}
			
			Object phoneMacObj = obj.get(keyMac);
			if (phoneMacObj == null) {
				context.getCounter(JobCounter.DB_PHONEMAC_NULL).increment(1);
				continue;
			}
			
			if (userTypeObj.toString().equals(Common.USERTYPE_MACHINE)) {
				MergeReducer.buildMap(shopId, phoneMacObj.toString(), machineMap);
				context.getCounter(JobCounter.MACHINE_MAP_OK).increment(1);
			} else if (userTypeObj.toString().equals(Common.USERTYPE_EMPLOYEE)) {
				MergeReducer.buildMap(shopId, phoneMacObj.toString(), employeeMap);
				context.getCounter(JobCounter.EMPLOYEE_MAP_OK).increment(1);
			}
		}
		
		DBCollection timeCollection = mongoDb.getCollection(timeCollectionName);
		BasicDBObject timeRef = new BasicDBObject();
		BasicDBObject timeKeys = new BasicDBObject();
		timeKeys.append(timeCollectionKeyStartTime, 1)
				.append(timeCollectionKeyEndTime, 1)
				.append(timeCollectionKeyShopId, 1);
		DBCursor timeCursor = timeCollection.find(timeRef, timeKeys);
		while (timeCursor.hasNext()) {
			DBObject obj = timeCursor.next();
			
			Object shopIdObj = obj.get(timeCollectionKeyShopId);
			if (shopIdObj == null) {
				context.getCounter(JobCounter.DB_TIMESHOPID_NULL).increment(1);
				continue;
			}
			int shopId = Integer.parseInt(shopIdObj.toString());
			
			Object startTimeObj = obj.get(timeCollectionKeyStartTime);
			if (startTimeObj == null) {
				context.getCounter(JobCounter.DB_TIMESTART_NULL).increment(1);
				continue;
			}
			long startTime = getTime(startTimeObj.toString());
			
			Object endTimeObj = obj.get(timeCollectionKeyEndTime);
			if (endTimeObj == null) {
				context.getCounter(JobCounter.DB_TIMEEND_NULL).increment(1);
				continue;
			}
			long endTime = getTime(endTimeObj.toString());
			
			List<Long> timeRange = new ArrayList<Long>();
			timeRange.add(startTime);
			timeRange.add(endTime);
			shopBusinessTimeMap.put(shopId, timeRange);
		}
		
		mongoClient.close();
		
		String featureStr = context.getConfiguration().get(Common.TRAIN_PASSENGER_FEATURELIST);
		int featureSize = 0;
		if (!featureStr.equals(Common.NULL)) {
			String arr[] = featureStr.split(Common.COMMA, -1);
			for (String str : arr) {
				featureList.add(Integer.parseInt(str));
			}
			featureSize = featureList.size();
		}
		else {
			context.getCounter(JobCounter.MODEL_TABLE_OFF).increment(1);
		}
		
		String filePath = context.getConfiguration().get(Common.TRAIN_MODEL_PATH);
		try {
			UserClassifyFunc.getClassifyMartix(filePath, featureSize, shopModelMap);
		} catch (Exception e) {
			featureList.clear();
			context.getCounter(JobCounter.MODEL_TABLE_ERROR).increment(1);
		}
		
		multipleOutputs = new MultipleOutputs<LongWritable, BytesWritable>(context);
	}

	@Override
	public void reduce(LongWritable key, Iterable<BytesWritable> values,
			Context context) throws IOException, InterruptedException {
		String outputIn  = "in";
		String outputOut = "out";
		Shop.Builder sb = Shop.newBuilder();
		Set<ByteString> apSet = new HashSet<ByteString>();
		Map<ByteString, Customer> custMap = new HashMap<ByteString, Customer>();

		// merge values together
		for (BytesWritable val : values) {
			sb.mergeFrom(val.getBytes(), 0, val.getLength());
		}
		context.getCounter(JobCounter.SHOP_OK).increment(1);

		for (ByteString ap : sb.getApMacList()) {
			apSet.add(ap);
		}
		sb.clearApMac();
		Iterator<ByteString> iterAp = apSet.iterator();
		while (iterAp.hasNext()) {
			sb.addApMac(iterAp.next());
		}

		context.getCounter(JobCounter.CUST_NUM_BEFORE).increment(
				sb.getCustomerCount());
		Set<Long> timeSet = new HashSet<Long>();
		//add repeated customer together, put into a map
		for (Customer.Builder cust : sb.getCustomerBuilderList()) {
			ByteString phoneMac = cust.getPhoneMac();

			if (custMap.containsKey(phoneMac)) {
				cust.mergeFrom(custMap.get(phoneMac));
			}
			custMap.put(phoneMac, cust.build());
		}
		context.getCounter(JobCounter.CUST_NUM_UNIQ).increment(custMap.size());

		Map<Long, Integer> timeRssiMap = new HashMap<Long, Integer>();			
		sb.clearCustomer();
		for (ByteString phoneMac : custMap.keySet()) {
			//for evey customer, get uniq timestamp and rssi
			Customer.Builder cb = Customer.newBuilder();
			cb.clear().mergeFrom(custMap.get(phoneMac));
			timeSet.clear();
			
			if (cb.getTimeStampCount() != cb.getApRssiCount()) {
				context.getCounter(JobCounter.TIME_RSSI_NOT_EQUAL).increment(1);
				continue;
			}
			//uniq time and get max rssi
			timeRssiMap.clear();
			for (int i = 0; i < cb.getTimeStampCount(); i++) {
				long timeStamp = cb.getTimeStamp(i);
				int rssi = cb.getApRssi(i);
				
				if (!timeRssiMap.containsKey(timeStamp)) {
					timeRssiMap.put(timeStamp, rssi);
				}
				else {
					int rssiMax = timeRssiMap.get(timeStamp);
					if (rssi > rssiMax) {
						timeRssiMap.put(timeStamp, rssi);
					}
				}
			}
			
			//sort time
			Iterator<Long> iterTime = timeRssiMap.keySet().iterator();
			List<Long> timeList = new ArrayList<Long>();
			while (iterTime.hasNext()) {
				timeList.add(iterTime.next());
			}
			Collections.sort(timeList);
			
			cb.clearTimeStamp();
			cb.clearApRssi();
			for (long timeStamp : timeList) {
				cb.addTimeStamp(timeStamp);
				cb.addApRssi(timeRssiMap.get(timeStamp));
			}
			

			//cb.setTimeDwell(cb.getTimeStampCount());
			sb.addCustomer(cb.build());
		}
		
		int shopId = sb.getShopId();
		for (Customer.Builder cb : sb.getCustomerBuilderList()) {
			if (MergeReducer.getRssiAvg(cb.getApRssiList()) < RSSI_FILTER) {
				//cb.setUserType(UserType.OUTDOOR);
				cb.setUserType(UserType.PASSENGER);
				continue;
			}
			else {
				context.getCounter(JobCounter.STRONG_RSSI).increment(1);
			}
			
			String phoneMac = new String(cb.getPhoneMac().toByteArray());
			if (machineMap.containsKey(shopId)
					&& machineMap.get(shopId).contains(phoneMac)) {
				cb.setUserType(UserType.MACHINE);
				context.getCounter(JobCounter.CUST_MACHINE).increment(1);
			}
			else if (employeeMap.containsKey(shopId)
					&& employeeMap.get(shopId).contains(phoneMac)) {
				cb.setUserType(UserType.EMPLOYEE);
				context.getCounter(JobCounter.CUST_EMPLOYEE).increment(1);
			}
			else if (featureList.size() == 0 
					|| !shopModelMap.containsKey(shopId)) {
				//classifier is off or no model for the shop
				if (cb.getTimeStampCount() <= DEFAULT_FILTER) {
					cb.setUserType(UserType.PASSENGER);
					context.getCounter(JobCounter.CUST_PASSENGER).increment(1);
				}
				else {
					cb.setUserType(UserType.CUSTOMER);
				}
			}
			else {	
				UserClassifyNew classify = shopModelMap.get(shopId);				
				if (UserType.PASSENGER == 
						classify.classifyPassenger(cb.build(), featureList)) {
					cb.setUserType(UserType.PASSENGER);
					context.getCounter(JobCounter.CUST_PASSENGER).increment(1);
				}
				else {
					cb.setUserType(UserType.CUSTOMER);
				}	
			}
				
		}
		
		for (ByteString bstr : sb.getApMacList()) {
			String ap = new String(bstr.toByteArray());
			apOKSet.add(ap);
		}

		LongWritable outKey = new LongWritable(sb.getShopId());
		BytesWritable outVal = new BytesWritable(sb.build().toByteArray());
		
		
		//if start time is not in businesstime, return.
		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		try {
			long timeStamp = timeFormat.parse(TIME_START).getTime();
			if (shopBusinessTimeMap.containsKey(shopId)) {
				List<Long> timeRange = shopBusinessTimeMap.get(shopId);

				if (timeStamp < timeRange.get(0) || timeStamp > timeRange.get(1)) {
					context.getCounter(JobCounter.OUT_OF_TIME_RANGE).increment(1);
					multipleOutputs.write(
							Common.BUSINESSTIME_OUT, outKey, outVal, outputOut);
				}
				else {
					multipleOutputs.write(
							Common.BUSINESSTIME_OUT, outKey, outVal, outputIn);
				}
				
			}
			else {
				context.getCounter(JobCounter.SHOP_WHITHOUT_BUSINESSTIME).increment(1);
			}
		} catch (ParseException e) {
			throw new RuntimeException("Parse time error:" + TIME_START);
		}
		
		//context.write(outKey, outVal);
	}

	@Override
	public void cleanup(Context context)
			throws IOException, InterruptedException {
		multipleOutputs.close();
		
		String mongoServerList = context.getConfiguration().get(
				Common.MONGO_SERVER_LIST);
		String serverArr[] = mongoServerList.split(Common.COMMA, -1);
		if (serverArr.length != Common.MONGO_SERVER_NUM) {
			throw new RuntimeException("Get mongo server fail.");
		}
		String mongoServerFst = serverArr[0];
		String mongoServerSnd = serverArr[1];
		String mongoServerTrd = serverArr[2];

		String mongoDbName = context.getConfiguration().get(
				Common.MONGO_DB_NAME);
		String apCollectionName = context.getConfiguration().get(
				Common.MONGO_COLLECTION_APMAC);
		String apCollectionKeyMac = context.getConfiguration().get(
				Common.MONGO_COLLECTION_APMAC_MAC);
		String apCollectionStatus = context.getConfiguration().get(
				Common.MONGO_COLLECTION_APMAC_STATUS);
		int mongoServerPort = Integer.parseInt(context.getConfiguration().get(
				Common.MONGO_SERVER_PORT));
		

		MongoClient mongoClient = new MongoClient(Arrays.asList(
				new ServerAddress(mongoServerFst, mongoServerPort),
				new ServerAddress(mongoServerSnd, mongoServerPort),
				new ServerAddress(mongoServerTrd, mongoServerPort)));
		DB mongoDb = mongoClient.getDB(mongoDbName);
		DBCollection apCollection = mongoDb.getCollection(apCollectionName);
		
		BasicDBObject apRef = new BasicDBObject();
		BasicDBObject apKeys = new BasicDBObject();
		apKeys.append(apCollectionKeyMac, 1);
		DBCursor apCursor = apCollection.find(apRef, apKeys);
		while (apCursor.hasNext()) {
			DBObject obj = apCursor.next();
			
			Object apMacObj = obj.get(apCollectionKeyMac);
			if (apMacObj == null) {
				continue;
			}
			context.getCounter(JobCounter.AP_TOTAL_NUM).increment(1);
			
			String apMac = apMacObj.toString().toLowerCase();
			BasicDBObject query = new BasicDBObject().append(apCollectionKeyMac, apMac);
			if (!apOKSet.contains(apMac)) {
				BasicDBObject update = new BasicDBObject().append(
						Common.MONGO_OPTION_SET,
						new BasicDBObject(apCollectionStatus, Common.AP_STATUS_FAIL));
				apCollection.update(query, update);
				context.getCounter(JobCounter.AP_FAIL_NUM).increment(1);
			}
			else {
				BasicDBObject update = new BasicDBObject().append(
						Common.MONGO_OPTION_SET,
						new BasicDBObject(apCollectionStatus, Common.AP_STATUS_OK));
				apCollection.update(query, update);
			}
		}
		
		mongoClient.close();
	}
	
	private long getTime(String time) {
		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);

		time = DATE + " " + time;
		long timeStamp = 0;
		try {
			timeStamp = timeFormat.parse(time).getTime();
		} catch (ParseException e) {
			throw new RuntimeException("Parse time error:" + time);
		}
		return timeStamp;
	}	
}
