package com.doodod.market.analyse;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.doodod.market.statistic.Common;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class MachineReducer extends
	Reducer<Text, Text, Text, Text> {
	enum JobCounter {
		KEY_FORMAT_ERROR,
		MACHINE_NEW_NUM,
		DB_SHOPID_NULL,
		DB_USERTYPE_NULL,
		DB_PHONEMAC_NULL,
		MACHINE_MAP_OK,
		EMPLOYEE_AT_NIGHT
	}
	
	private static String CREATE_TIME = "create_time";
	private static int MACHINE_COUNT_FILTEER = 0;
	private static int RSSI_VAR_MACHINE = 10;


	private static Map<Integer, Set<String>> machineInDb = new HashMap<Integer, Set<String>>();
	private static Map<Integer, List<String>> machineAtNight = new HashMap<Integer, List<String>>();

	private static MongoClient mongoClient;
	private static String mongoDbName;
	private static String typeCollectionName;
	private static String keyShopId;
	private static String keyType;
	private static String keyMac;
	
	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		RSSI_VAR_MACHINE = Integer.parseInt(
				context.getConfiguration().get(Common.RSSI_MACHINE_VAR));
		MACHINE_COUNT_FILTEER = Integer.parseInt(
				context.getConfiguration().get(Common.MACHINE_COUNT_FILTER));
		CREATE_TIME = context.getConfiguration().get(Common.MONGO_COLLECTION_USER_TIME);

		String mongoServerList = context.getConfiguration().get(
				Common.MONGO_SERVER_LIST);
		String serverArr[] = mongoServerList.split(Common.COMMA, -1);
		if (serverArr.length != Common.MONGO_SERVER_NUM) {
			throw new RuntimeException("Get mongo server fail.");
		}
		String mongoServerFst = serverArr[0];
		String mongoServerSnd = serverArr[1];
		String mongoServerTrd = serverArr[2];

		mongoDbName = context.getConfiguration().get(
				Common.MONGO_DB_NAME);
		int mongoServerPort = Integer.parseInt(context.getConfiguration().get(
				Common.MONGO_SERVER_PORT));

		mongoClient = new MongoClient(Arrays.asList(
				new ServerAddress(mongoServerFst, mongoServerPort),
				new ServerAddress(mongoServerSnd, mongoServerPort),
				new ServerAddress(mongoServerTrd, mongoServerPort)));
		
		typeCollectionName = context.getConfiguration().get(
				Common.MONGO_COLLECTION_USERINFO);
		keyShopId = context.getConfiguration().get(
				Common.MONGO_COLLECTION_USER_SHOPID);
		keyType = context.getConfiguration().get(
				Common.MONGO_COLLECTION_USER_TYPE);
		keyMac = context.getConfiguration().get(
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
				buildMap(shopId, phoneMacObj.toString(), machineInDb);
				context.getCounter(JobCounter.MACHINE_MAP_OK).increment(1);
			} 
		}
	}


	@Override
	public void reduce(Text key, Iterable<Text> values,
		Context context) throws IOException, InterruptedException {
		DecimalFormat numFormat = new DecimalFormat(Common.NUM_FORNAT);

		String arr[] = key.toString().split(Common.CTRL_A, -1);
		if (arr.length != 2) {
			context.getCounter(JobCounter.KEY_FORMAT_ERROR).increment(1);
			return;
		}
		
		int shopId = Integer.parseInt(arr[0]);
		String phoneMac = arr[1];
		String phoneBrand = "";
		float rssVars = 0;
		int count = 0;
		for (Text val : values) {
			String valArr[] = val.toString().split(Common.CTRL_A, -1);
			phoneBrand = valArr[0];
			rssVars += Float.parseFloat(valArr[1]);
			count ++;
		}
		
		if (count < MACHINE_COUNT_FILTEER) {
			return;
		}
		
		rssVars /= count;
		if (rssVars > RSSI_VAR_MACHINE ) {
			context.getCounter(JobCounter.EMPLOYEE_AT_NIGHT).increment(1);
			return;
		}
		
		if (machineInDb.containsKey(shopId) 
				&& machineInDb.get(shopId).contains(phoneMac)) {
			context.write(key, new Text(phoneBrand + Common.CTRL_A 
					+ String.valueOf(count) + Common.CTRL_A + "OLD"
					+ Common.CTRL_A + numFormat.format(rssVars)));
		}
		else {	
			if (machineAtNight.containsKey(shopId)) {
				machineAtNight.get(shopId).add(phoneMac);
			}
			else {
				List<String> macList = new ArrayList<String>();
				macList.add(phoneMac);
				machineAtNight.put(shopId, macList);
			}
			
			context.write(key, new Text(phoneBrand + Common.CTRL_A 
					+ String.valueOf(count) + Common.CTRL_A + "NEW"
					+ Common.CTRL_A + numFormat.format(rssVars)));
		}


	}
	
	
	@Override
	public void cleanup(Context context)
			throws IOException, InterruptedException {	
		long timeUp = Long.valueOf(context.getConfiguration()
				.get(Common.BUSINESSTIME_NOW));
			
		DB mongoDb = mongoClient.getDB(mongoDbName);
		DBCollection typeCollection = mongoDb.getCollection(typeCollectionName);
		
		Iterator<Integer> iter = machineAtNight.keySet().iterator();
		while (iter.hasNext()) {
			int shopId = iter.next();
			List<String> macList = machineAtNight.get(shopId);
			Set<String> macInDb = machineInDb.get(shopId);
			
			if (macInDb == null) {
				macInDb = new HashSet<String>();
			}
			
			for (String mac : macList) {
				if (!macInDb.contains(mac)) {
					context.getCounter(JobCounter.MACHINE_NEW_NUM).increment(1);
					
					BasicDBObject obj = new BasicDBObject();
					obj.append(keyShopId, String.valueOf(shopId));
					obj.append(keyMac, mac);
					obj.append(keyType, 
							Integer.parseInt(Common.USERTYPE_MACHINE));
					obj.append(CREATE_TIME, timeUp);
					
					typeCollection.insert(obj);
				}
			}
		}
		
		mongoClient.close();
	}

	private void buildMap(int shopId, String phoneMac,
			Map<Integer, Set<String>> dstMap) {
		if (dstMap.containsKey(shopId)) {
			dstMap.get(shopId).add(phoneMac);
		} else {
			Set<String> macSet = new HashSet<String>();
			macSet.add(phoneMac);
			dstMap.put(shopId, macSet);
		}
	}
}
