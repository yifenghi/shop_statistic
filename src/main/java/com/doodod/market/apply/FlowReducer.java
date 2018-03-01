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
import org.apache.hadoop.mapreduce.Reducer;

import com.doodod.market.message.Store.Customer;
import com.doodod.market.message.Store.Shop;
import com.doodod.market.message.Store.UserType;
import com.doodod.market.statistic.Common;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class FlowReducer extends 
	Reducer<LongWritable, BytesWritable, LongWritable, Text> {
	
	private static String TIME_TAG;
	private static String CUSTOMER_TAG;
	private static String PASSENGER_TAG;
	private static String EMPLOYEE_TAG;
	private static String MACHINE_TAG;
	private static String TOTAL_TAG;
	private static String CUSTOMER_TYPE;
	private static String PASSENGER_TYPE;
	private static String EMPLOYEE_TYPE;
	private static String MACHINE_TYPE;
	private static String TOTAL_TYPE;
	private static String KEY_CREATETIME;
	private static String KEY_SHOPID;

	private static final String TIME_TAG_MIN = "min";
	private static final String TIME_TAG_HOUR = "hour";
	private static final String TIME_TAG_DAY = "day";
	private static long TIME_STAMP = 0;
	
	private static Map<Integer, DBObject> shopFlowMap = 
			new HashMap<Integer, DBObject>();
	
	enum JobCounter {
		//TODO counter
		CUSTOMER_NUM,
		PASSENGER_NUM,
		EMPLOYEE_NUM,
		MACHINE_NUM,
		TOTAL_NUM,
		USER_TYPE_ERROR,
		DB_WRITE_OK,
		TIME_TAG_ERROR,
		TIME_TAG_MIN_OK,
		TIME_TAG_HOUR_OK,
		TIME_TAG_DAY_OK
	}
	
	@Override
	public void setup(Context context) throws IOException,
			InterruptedException {
		KEY_SHOPID = context.getConfiguration().get(
				Common.MONGO_COLLECTION_MALL_SHOPID);
		KEY_CREATETIME = context.getConfiguration().get(
				Common.MONGO_COLLECTION_MALL_TIME);
		
		CUSTOMER_TYPE = context.getConfiguration().get(Common.USERTYPE_TAG_CUSTOMER);
		PASSENGER_TYPE = context.getConfiguration().get(Common.USERTYPE_TAG_PASSENGER);
		EMPLOYEE_TYPE =  context.getConfiguration().get(Common.USERTYPE_TAG_EMPLOYEE);
		MACHINE_TYPE = context.getConfiguration().get(Common.USERTYPE_TAG_MACHINE);
		TOTAL_TYPE = context.getConfiguration().get(Common.USERTYPE_TAG_TOTAL);
				
		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		try {
			TIME_STAMP = timeFormat.parse(context.getConfiguration().get(
					Common.MALL_INFO_TIMESTAMP)).getTime();
			TIME_STAMP --;
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
		
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
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
		
		String mallCollectionName = context.getConfiguration().get(
				Common.MONGO_COLLECTION_MALLINFO);
		DB mongoDb = mongoClient.getDB(mongoDbName);
		DBCollection mallCollection = 
				mongoDb.getCollection(mallCollectionName);

		Iterator<Integer> iter = shopFlowMap.keySet().iterator();
		while (iter.hasNext()) {
			int shopId = iter.next();
            DBObject obj = shopFlowMap.get(shopId);	
            obj.put(KEY_CREATETIME, TIME_STAMP);
            
            BasicDBObject query = new BasicDBObject();
            query.append(KEY_CREATETIME, TIME_STAMP);
            query.append(KEY_SHOPID, obj.get(KEY_SHOPID));
            
            mallCollection.update(query, obj, true, false);
            context.getCounter(JobCounter.DB_WRITE_OK).increment(1);
		}
		
		mongoClient.close();
	}

	@Override
	public void reduce(LongWritable arg0, Iterable<BytesWritable> values,
			Context context)
			throws IOException, InterruptedException {
		DBObject obj = new BasicDBObject();
		int shopId = 0;

		for (BytesWritable val : values) {
			byte[] arr = val.getBytes();
			switch (arr[0]) {
			case Common.FLOW_TAG_M:
				TIME_TAG = TIME_TAG_MIN;
				context.getCounter(JobCounter.TIME_TAG_MIN_OK).increment(1);
				break;
			case Common.FLOW_TAG_H:
				TIME_TAG = TIME_TAG_HOUR;
				context.getCounter(JobCounter.TIME_TAG_HOUR_OK).increment(1);
				break;	
			case Common.FLOW_TAG_D:
				TIME_TAG = TIME_TAG_DAY;
				context.getCounter(JobCounter.TIME_TAG_DAY_OK).increment(1);
				break;
			default:
				context.getCounter(JobCounter.TIME_TAG_ERROR).increment(1);
				break;
			}
			
			CUSTOMER_TAG  = TIME_TAG + Common.UNDER_LINE + CUSTOMER_TYPE;
			PASSENGER_TAG = TIME_TAG + Common.UNDER_LINE + PASSENGER_TYPE;
			EMPLOYEE_TAG  = TIME_TAG + Common.UNDER_LINE + EMPLOYEE_TYPE;
			MACHINE_TAG   = TIME_TAG + Common.UNDER_LINE + MACHINE_TYPE;
			TOTAL_TAG     = TOTAL_TYPE + Common.UNDER_LINE + TIME_TAG;
			 
			Shop.Builder sb = Shop.newBuilder();
			sb.mergeFrom(arr, 1, val.getLength() - 1);
			shopId = sb.getShopId();
			
			int customerNum  = 0;
			int passengerNum = 0;
			int employeeNum  = 0;
			int machineNum   = 0;
			int totalNum = sb.getCustomerCount();
			for (Customer cust : sb.getCustomerList()) {
				switch (cust.getUserType().getNumber()) {
				case UserType.CUSTOMER_VALUE:
					customerNum ++;
					context.getCounter(JobCounter.CUSTOMER_NUM).increment(1);
					break;
				case UserType.PASSENGER_VALUE:
					passengerNum ++;
					context.getCounter(JobCounter.PASSENGER_NUM).increment(1);
					break;
				case UserType.EMPLOYEE_VALUE:
					employeeNum ++;
					context.getCounter(JobCounter.EMPLOYEE_NUM).increment(1);
					break;
				case UserType.MACHINE_VALUE:
					machineNum ++;
					context.getCounter(JobCounter.MACHINE_NUM).increment(1);
					break;
				default:
					context.getCounter(JobCounter.USER_TYPE_ERROR).increment(1);
					break;
				}
			}
			
			obj.put(CUSTOMER_TAG, customerNum);
			obj.put(PASSENGER_TAG, passengerNum);
			obj.put(EMPLOYEE_TAG, employeeNum);
			obj.put(MACHINE_TAG, machineNum);
			obj.put(TOTAL_TAG, totalNum);
		}
		
        obj.put(KEY_SHOPID, shopId);
		shopFlowMap.put(shopId, obj);
	}
	
}
