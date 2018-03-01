/**
 * @author caoyupeng@doodod.com
 */
package com.doodod.market.statistic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.doodod.market.analyse.UserClassifyNew;
import com.doodod.market.message.Store.Customer;
import com.doodod.market.message.Store.Shop;
import com.doodod.market.message.Store.UserType;
import com.google.protobuf.ByteString;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class MergeReducer extends
		Reducer<LongWritable, BytesWritable, LongWritable, BytesWritable> {
	enum JobCounter {
		TOTAL_REDUCE_OK,
		PART_REDUCE_OK,
		INVALID_RECORD, 
		CUST_REIMIAN, 
		CUST_NEWCOME, 
		CUST_TOTAL, 
		CUST_MACHINE, 
		CUST_EMPLOYEE, 
		CUST_PASSENGER, 
		NEWCUST_NUM_ERROR, 
		MODEL_TABLE_OFF,
		MODEL_TABLE_ERROR,
		DB_SHOPID_NULL,
		DB_USERTYPE_NULL,
		DB_PHONEMAC_NULL,
		MACHINE_MAP_OK,
		EMPLOYEE_MAP_OK,
		STRONG_RSSI
	}
	
	private static int DEFAULT_FILTER = 0;
	private static int RSSI_FILTER = 0;

	private static List<Integer> featureList = new ArrayList<Integer>();
	private static Map<Integer, UserClassifyNew> shopModelMap = new HashMap<Integer, UserClassifyNew>();
	private static Map<Integer, Set<String>> machineMap = new HashMap<Integer, Set<String>>();
	private static Map<Integer, Set<String>> employeeMap = new HashMap<Integer, Set<String>>();

	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		RSSI_FILTER = Integer.parseInt(
				context.getConfiguration().get(Common.RSSI_INDOOR_SCOPE));
		
		String featureStr = context.getConfiguration().get(Common.TRAIN_PASSENGER_FEATURELIST);
		DEFAULT_FILTER = Integer.parseInt(
				context.getConfiguration().get(Common.TRAIN_PASSENGER_DEFAULT_FILTER));
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
				buildMap(shopId, phoneMacObj.toString(), machineMap);
				context.getCounter(JobCounter.MACHINE_MAP_OK).increment(1);
			} else if (userTypeObj.toString().equals(Common.USERTYPE_EMPLOYEE)) {
				buildMap(shopId, phoneMacObj.toString(), employeeMap);
				context.getCounter(JobCounter.EMPLOYEE_MAP_OK).increment(1);
			}
		}
	}

	@Override
	public void reduce(LongWritable key, Iterable<BytesWritable> values,
			Context context) throws IOException, InterruptedException {
		Shop.Builder total = Shop.newBuilder();
		Shop.Builder part = Shop.newBuilder();

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

		// when total is empty, get info from part
		if (!total.hasShopId()) {
			total.setShopId(part.getShopId()).clearApMac()
					.addAllApMac(part.getApMacList());
		}

		Map<ByteString, Integer> custMap = new HashMap<ByteString, Integer>();
		int partCustIndex = 0;
		for (Customer cust : part.getCustomerList()) {
			custMap.put(cust.getPhoneMac(), partCustIndex);
			partCustIndex++;
		}

		List<Integer> custIndexMatch = new ArrayList<Integer>();
		int matchCount = 0;
		for (Customer.Builder cb : total.getCustomerBuilderList()) {
			ByteString phoneMac = cb.getPhoneMac();
			if (custMap.containsKey(phoneMac)) {
				matchCount++;

				int index = custMap.get(phoneMac);
				for (long timeStamp : part.getCustomer(index).getTimeStampList()) {
					cb.addTimeStamp(timeStamp);
				}
				for (int rssi : part.getCustomer(index).getApRssiList()) {
					cb.addApRssi(rssi);
				}
				//cb.setTimeDwell(cb.getTimeStampCount());
				custIndexMatch.add(index);

				context.getCounter(JobCounter.CUST_REIMIAN).increment(1);
			}
		}

		int custumerLast = total.getCustomerCount();
		for (int i = 0; i < part.getCustomerCount(); i++) {
			if (custIndexMatch.contains(i)) {
				continue;
			}
			total.addCustomer(part.getCustomer(i));
		}

		int index = 0;
		int shopId = total.getShopId();
		
		for (Customer.Builder cb : total.getCustomerBuilderList()) {
			if (getRssiAvg(cb.getApRssiList()) < RSSI_FILTER) {
				//cb.setUserType(UserType.OUTDOOR);
				cb.setUserType(UserType.PASSENGER);
				continue;
			}
			else {
				context.getCounter(JobCounter.STRONG_RSSI).increment(1);
			}
			
			String phoneMac = new String(cb.getPhoneMac().toByteArray());
			if (cb.getUserType().equals(UserType.MACHINE)) {
				context.getCounter(JobCounter.CUST_MACHINE).increment(1);;
			} else if (cb.getUserType().equals(UserType.EMPLOYEE)) {
				context.getCounter(JobCounter.CUST_EMPLOYEE).increment(1);;
			} else if (machineMap.containsKey(shopId)
					&& machineMap.get(shopId).contains(phoneMac)) {
				context.getCounter(JobCounter.CUST_MACHINE).increment(1);;
				cb.setUserType(UserType.MACHINE);
			} else if (employeeMap.containsKey(shopId) 
					&& employeeMap.get(shopId).contains(phoneMac)) {
				context.getCounter(JobCounter.CUST_EMPLOYEE).increment(1);
				cb.setUserType(UserType.EMPLOYEE);
			} else if (featureList.size() == 0 
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

			if (index >= custumerLast
					&& !cb.getUserType().equals(UserType.CUSTOMER)) {
				// if new comer isn't customer, do not add to new customer.
				matchCount++;
			}
		}

		int newCustCount = partCustIndex - matchCount;
		if (newCustCount < 0) {
			context.getCounter(JobCounter.NEWCUST_NUM_ERROR).increment(1);
			newCustCount = 0;
		}
		total.setNewCustomerNum(newCustCount);
		context.getCounter(JobCounter.CUST_NEWCOME).increment(newCustCount);
		context.getCounter(JobCounter.CUST_TOTAL).increment(
				total.getCustomerCount());

		BytesWritable res = new BytesWritable(total.build().toByteArray());
		context.write(key, res);
	}
	
	public static void buildMap(int shopId, String phoneMac,
			Map<Integer, Set<String>> dstMap) {
		if (dstMap.containsKey(shopId)) {
			dstMap.get(shopId).add(phoneMac);
		} else {
			Set<String> macSet = new HashSet<String>();
			macSet.add(phoneMac);
			dstMap.put(shopId, macSet);
		}
	}
	
	public static int getRssiAvg(List<Integer> list) {
		if (list.size() == 0) {
			return -100;
		}
		
		int rssiAvg = 0;
		for (int rssi :list) {
			rssiAvg += rssi;
		}
		rssiAvg = rssiAvg / list.size();
		return rssiAvg;
	}

}
