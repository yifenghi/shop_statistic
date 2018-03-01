package com.doodod.market.apply;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.doodod.market.statistic.Common;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class VisitShopInfoReducer extends 
	Reducer<LongWritable, Text, LongWritable, Text> {
	private static String KEY_SHOPID;
	private static String KEY_CREATETIME;
	private static String NEW_CLIENT_NUM;
	private static String OLD_CLIENT_NUM;
	private static long DATE_TIME;
	private Map<Long, BasicDBObject> CUST_NUM_MAP = new HashMap<Long, BasicDBObject>();
	
	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		try {
			DATE_TIME = timeFormat.parse(
					context.getConfiguration().get(Common.BIZDATE))
					.getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		KEY_SHOPID = context.getConfiguration().get(
				Common.MONGO_COLLECTION_MALL_SHOPID);
		KEY_CREATETIME = context.getConfiguration().get(
				Common.MONGO_COLLECTION_MALL_TIME);
		NEW_CLIENT_NUM = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_NEW);
		OLD_CLIENT_NUM = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_OLD);
		
	}
	
	@Override
	public void cleanup(Context context)
			throws IOException, InterruptedException {
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
		
		String userCollectionName = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISITINFO);
		DB mongoDb = mongoClient.getDB(mongoDbName);
		DBCollection userCollection = 
				mongoDb.getCollection(userCollectionName);
		
		Iterator<Long> iter = CUST_NUM_MAP.keySet().iterator();
		while (iter.hasNext()) {
			long shopId = iter.next();
			BasicDBObject document = CUST_NUM_MAP.get(shopId);
			BasicDBObject docSet = new BasicDBObject();
			docSet.append(Common.MONGO_OPTION_SET, document);

			BasicDBObject query = new BasicDBObject();
			query.append(KEY_CREATETIME, DATE_TIME);
			query.append(KEY_SHOPID, shopId);
			
			userCollection.update(query, docSet, true, false);
		}
		mongoClient.close();
	}
	
	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int newCustSum = 0;
		int oldCustSum = 0;
		
		for (Text val : values) {
			String arr[] = val.toString().split(Common.CTRL_A, -1);
			newCustSum += Integer.parseInt(arr[0]);
			oldCustSum += Integer.parseInt(arr[1]);
		}
		
		BasicDBObject document = new BasicDBObject();
		document.put(NEW_CLIENT_NUM, newCustSum);
		document.put(OLD_CLIENT_NUM, oldCustSum);
		
		long shopId = key.get();
		CUST_NUM_MAP.put(shopId, document);
		context.write(key, new Text(newCustSum + Common.CTRL_A + oldCustSum));
	}
	
	

}
