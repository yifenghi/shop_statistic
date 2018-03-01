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

import com.doodod.market.apply.FlowReducer.JobCounter;
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

public class FlowMapper extends 
	Mapper<LongWritable, BytesWritable, LongWritable, Text> {
	private static long TIMESTAMP_MIN = 0;
	private static long TIMESTAMP_HOUR = 0;
	private static long TIMESTAMP_CREATE = 0;

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
	
	private static Map<Integer, BasicDBObject> MIN_CUST_MAP = new HashMap<Integer, BasicDBObject>();
	private static Map<Integer, BasicDBObject> HOUR_CUST_MAP = new HashMap<Integer, BasicDBObject>();
	private static Map<Integer, BasicDBObject> DAY_CUST_MAP = new HashMap<Integer, BasicDBObject>();

	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
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
			TIMESTAMP_MIN = timeFormat.parse(context.getConfiguration().get(
					Common.TIME_TAG_MIN)).getTime() - 1;
			TIMESTAMP_HOUR = timeFormat.parse(context.getConfiguration().get(
					Common.TIME_TAG_HOUR)).getTime() - 1;
			TIMESTAMP_CREATE = timeFormat.parse(context.getConfiguration().get(
					Common.MALL_INFO_TIMESTAMP)).getTime() - 1;
		} catch (ParseException e) {
			e.printStackTrace();
		}
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
		
		String mallCollectionName = context.getConfiguration().get(
				Common.MONGO_COLLECTION_MALLINFO);
		DB mongoDb = mongoClient.getDB(mongoDbName);
		DBCollection mallCollection = 
				mongoDb.getCollection(mallCollectionName);
		
		Iterator<Integer> iterMin = MIN_CUST_MAP.keySet().iterator();
		while (iterMin.hasNext()) {
			int shopId = iterMin.next();
            DBObject obj = MIN_CUST_MAP.get(shopId);	
            obj.put(KEY_CREATETIME, TIMESTAMP_CREATE);
            
			BasicDBObject docSet = new BasicDBObject();
			docSet.append(Common.MONGO_OPTION_SET, obj);
            
            BasicDBObject query = new BasicDBObject();
            query.append(KEY_CREATETIME, TIMESTAMP_CREATE);
            query.append(KEY_SHOPID, shopId);
            
            mallCollection.update(query, docSet, true, false);
            context.getCounter(JobCounter.DB_WRITE_OK).increment(1);
		}
		
		Iterator<Integer> iterHour = HOUR_CUST_MAP.keySet().iterator();
		while (iterHour.hasNext()) {
			int shopId = iterHour.next();
            DBObject obj = HOUR_CUST_MAP.get(shopId);	
            obj.put(KEY_CREATETIME, TIMESTAMP_CREATE);
            
			BasicDBObject docSet = new BasicDBObject();
			docSet.append(Common.MONGO_OPTION_SET, obj);
            
            BasicDBObject query = new BasicDBObject();
            query.append(KEY_CREATETIME, TIMESTAMP_CREATE);
            query.append(KEY_SHOPID, shopId);
            mallCollection.update(query, docSet, true, false);
		}
		
		Iterator<Integer> iterDay = DAY_CUST_MAP.keySet().iterator();
		while (iterDay.hasNext()) {
			int shopId = iterDay.next();
			DBObject obj = DAY_CUST_MAP.get(shopId);
			obj.put(KEY_CREATETIME, TIMESTAMP_CREATE);
			
			BasicDBObject docSet = new BasicDBObject();
			docSet.append(Common.MONGO_OPTION_SET, obj);
			
			BasicDBObject query = new BasicDBObject();
			query.append(KEY_CREATETIME, TIMESTAMP_CREATE);
			query.append(KEY_SHOPID, shopId);
			mallCollection.update(query, docSet, true, false);
		}
		
		mongoClient.close();
	}
	
	@Override
	public void map(LongWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Shop.Builder sb = Shop.newBuilder();
		sb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		
		int shopId = sb.getShopId();
		
		VisiterNum visitMin = new VisiterNum(TIME_TAG_MIN);
		VisiterNum visitHour = new VisiterNum(TIME_TAG_HOUR);
		VisiterNum visitDay = new VisiterNum(TIME_TAG_DAY);
		
		for (Customer cust : sb.getCustomerList()) {
			int timeListSize = cust.getTimeStampCount();
			long timeLatest = cust.getTimeStamp(timeListSize - 1); 
			
			if (timeLatest > TIMESTAMP_MIN) {
				visitMin.incre(cust);
			}
			
			if (timeLatest > TIMESTAMP_HOUR) {
				visitHour.incre(cust);
			}
			
			visitDay.incre(cust);
		}
		
		MIN_CUST_MAP.put(shopId, visitMin.buildMongoDoc());
		HOUR_CUST_MAP.put(shopId, visitHour.buildMongoDoc());
		DAY_CUST_MAP.put(shopId, visitDay.buildMongoDoc());
		
		Text outVal = new Text(
				visitMin.getTimeTag() + Common.CTRL_B + visitMin.getTotalNum()
				+ Common.CTRL_A + visitHour.getTimeTag() + Common.CTRL_B + visitHour.getTotalNum()
				+ Common.CTRL_A + visitDay.getTimeTag() + Common.CTRL_B + visitDay.getTotalNum());
		context.write(key, outVal);
	}
	
	public class VisiterNum {
		private int customerNum;
		private int passengerNum;
		private int employeeNum;
		private int machineNum;
		private String timeTag;
		
		public VisiterNum(String tag) {
			timeTag = tag;
			customerNum = 0;
			passengerNum = 0;
			employeeNum = 0;
			machineNum = 0;
		}
		
		private void customerIncre() {
			customerNum ++;
		}
		
		private void passengerIncre() {
			passengerNum ++;
		}
		
		private void employeeIncre() {
			employeeNum ++;
		}
		
		private void machieIncre() {
			machineNum ++;
		} 
		
		public int getCustomerNum() {
			return customerNum;
		}
		
		public int getPassengerNum() {
			return passengerNum;
		}
		
		public int getEmployeeNum() {
			return employeeNum;
		}
		
		public int getMachineNum() {
			return machineNum;
		}
		
		public int getTotalNum() {
			return customerNum + passengerNum + employeeNum + machineNum;
		}
		
		public String getTimeTag() {
			return timeTag;
		}
		
		public void incre(Customer cust) {
			switch (cust.getUserType().getNumber()) {
			case UserType.CUSTOMER_VALUE:
				customerIncre();
				break;
			case UserType.PASSENGER_VALUE:
				passengerIncre();
				break;
			case UserType.EMPLOYEE_VALUE:
				employeeIncre();
				break;
			case UserType.MACHINE_VALUE:
				machieIncre();
				break;
			default:
				break;
			}
		}
		
		public BasicDBObject buildMongoDoc() {
			BasicDBObject obj = new BasicDBObject();
			obj.put(timeTag + Common.UNDER_LINE + CUSTOMER_TYPE, customerNum);
			obj.put(timeTag + Common.UNDER_LINE + PASSENGER_TYPE, passengerNum);
			obj.put(timeTag + Common.UNDER_LINE + EMPLOYEE_TYPE, employeeNum);
			obj.put(timeTag + Common.UNDER_LINE + MACHINE_TYPE, machineNum);
			obj.put(TOTAL_TYPE + Common.UNDER_LINE + timeTag, 
					customerNum + passengerNum + employeeNum + machineNum);
			return obj;
		}
	}
	
	

}
