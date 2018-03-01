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

public class VisitCustomerReducer extends
	Reducer<LongWritable, Text, LongWritable, Text> {
	private static long DATE_TIME = 0;
	private static String KEY_MALLID;
	private static String KEY_CREATETIME;
	private static String NUM_CUSTOMER;
	private static String NUM_EMPLOYEE;
	private static String NUM_MACHINE;
	private static String NUM_PASSENGER;
	private static String NUM_TOTAL;
	
	private static Map<Long, BasicDBObject> MALL_COUNT_MAP = new HashMap<Long, BasicDBObject>();

	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		NUM_CUSTOMER = context.getConfiguration().get(Common.MONGO_COLLECTION_COUNT_CUSTOMER);
		NUM_EMPLOYEE = context.getConfiguration().get(Common.MONGO_COLLECTION_COUNT_EMPLOYEE);
		NUM_MACHINE = context.getConfiguration().get(Common.MONGO_COLLECTION_COUNT_MACHINE);
		NUM_PASSENGER = context.getConfiguration().get(Common.MONGO_COLLECTION_COUNT_PASSENGER);
		NUM_TOTAL = context.getConfiguration().get(Common.MONGO_COLLECTION_COUNT_TOTAL);
	}
	
	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int customerNum = 0;
		int employeeNum = 0;
		int machineNum = 0;
		int passengerNum = 0;
		int total = 0;
		
		for (Text value : values) {
			String arr[] = value.toString().split(Common.CTRL_A, -1);
			customerNum += Integer.parseInt(arr[0]);
			employeeNum += Integer.parseInt(arr[1]);
			machineNum += Integer.parseInt(arr[2]);
			passengerNum += Integer.parseInt(arr[3]);
			total += Integer.parseInt(arr[4]);
		}
		
		BasicDBObject document = new BasicDBObject();
		document.put(NUM_CUSTOMER, customerNum);
		document.put(NUM_EMPLOYEE, employeeNum);
		document.put(NUM_MACHINE, machineNum);
		document.put(NUM_PASSENGER, passengerNum);
		document.put(NUM_TOTAL, total);
		MALL_COUNT_MAP.put(key.get(), document);
		
		Text outVal = new Text(customerNum + Common.CTRL_A 
				+ employeeNum + Common.CTRL_A 
				+ machineNum + Common.CTRL_A
				+ passengerNum + Common.CTRL_A
				+ total);
		
		context.write(key, outVal);
	}
	
	@Override
	public void cleanup(Context context)
			throws IOException, InterruptedException {
		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		try {
			DATE_TIME = timeFormat.parse(
					context.getConfiguration().get(Common.BIZDATE)).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}

		KEY_MALLID = context.getConfiguration().get(
				Common.MONGO_COLLECTION_COUNT_ID);
		KEY_CREATETIME = context.getConfiguration().get(
				Common.MONGO_COLLECTION_COUNT_TIME);

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

		String countCollectionName = context.getConfiguration().get(
				Common.MONGO_COLLECTION_COUNT);
		DB mongoDb = mongoClient.getDB(mongoDbName);
		DBCollection countCollection = mongoDb
				.getCollection(countCollectionName);

		Iterator<Long> iter = MALL_COUNT_MAP.keySet().iterator();
		while (iter.hasNext()) {
			long shopId = iter.next();
			BasicDBObject doc = MALL_COUNT_MAP.get(shopId);

			BasicDBObject query = new BasicDBObject();
			query.put(KEY_MALLID, shopId);
			query.put(KEY_CREATETIME, DATE_TIME);

			BasicDBObject docment = new BasicDBObject();
			docment.append(Common.MONGO_OPTION_SET, doc);

			countCollection.update(query, docment, true, false);
		}
		mongoClient.close();
	}

}
