/**
 * @author caoyupeng@doodod.com
 */
package com.doodod.market.statistic;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;

import com.doodod.market.message.Store.Customer;
import com.doodod.market.message.Store.Shop;
import com.google.protobuf.ByteString;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class ShopMapper extends TableMapper<LongWritable, BytesWritable> {
	enum JobCounter {
		MAP_TOTAL, 
		AP_NOT_IN_MAP, 
		TIME_CELL_IS_EMPTY, 
		DB_APMAC_NULL,
		DB_APSHOPID_NULL,
		BRAND_MAP_ERROR,
		MAC_NOT_IN_BRAND_MAP
	}


	private static Map<String, Integer> apShopMap = new HashMap<String, Integer>();
	private static Map<String, String> macBrandMap = new HashMap<String, String>();


	@Override
	public void setup(Context context) throws IOException, InterruptedException {
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
		String apCollectionKeyShopId = context.getConfiguration().get(
				Common.MONGO_COLLECTION_APMAC_SHOPID);
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
		apKeys.append(apCollectionKeyMac, 1).append(apCollectionKeyShopId, 1);
		DBCursor apCursor = apCollection.find(apRef, apKeys);
		while (apCursor.hasNext()) {
			DBObject obj = apCursor.next();
			
			Object apMacObj = obj.get(apCollectionKeyMac);
			if (apMacObj == null) {
				context.getCounter(JobCounter.DB_APMAC_NULL).increment(1);
				continue;
			}			
			Object apShopIdObj = obj.get(apCollectionKeyShopId);
			if (apShopIdObj == null) {
				context.getCounter(JobCounter.DB_APSHOPID_NULL).increment(1);
				continue;
			}
			
			// if conflict，hash map will update the map
			apShopMap.put(apMacObj.toString().toLowerCase(), 
					Integer.parseInt(apShopIdObj.toString()));
		}
		// close mongo client when reading fish
		mongoClient.close();
		
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
		}
		brandReader.close();
	}

	@Override
	public void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		Shop.Builder sb = Shop.newBuilder();

		byte[] macArr = key.get();
		String apMac = getMacFromArr(macArr, 0, Common.AP_MAC_LENGTH);
		String phoneMac = getMacFromArr(macArr, Common.AP_MAC_LENGTH,
				macArr.length);
		
		String phoneMacKey = phoneMac.substring(0, Common.MAC_KEY_LENGTH);
		String phoneBrand = Common.BRAND_NAME_UNKNOWN;
		if (macBrandMap.containsKey(phoneMacKey)) {
			phoneBrand = macBrandMap.get(phoneMacKey);
		}
		else {
			context.getCounter(JobCounter.MAC_NOT_IN_BRAND_MAP).increment(1);
		}

		context.getCounter(JobCounter.MAP_TOTAL).increment(1);
		if (!apShopMap.containsKey(apMac)) {
			context.getCounter(JobCounter.AP_NOT_IN_MAP).increment(1);
			return;
		}
		int shopId = apShopMap.get(apMac);
				
		sb.clear().setShopId(shopId)
				.addApMac(ByteString.copyFrom(apMac.getBytes()));

		Customer.Builder cb = Customer.newBuilder();
		cb.clear()
			.setPhoneMac(ByteString.copyFrom(phoneMac.getBytes()))
			.setPhoneBrand(ByteString.copyFrom(phoneBrand.getBytes()));
		
		Map<Long, Integer> timeRssiMap = new HashMap<Long, Integer>();
		for (Cell cell : value.listCells()) {
			long timeStamp = cell.getTimestamp();

			// covert time stamp to minutes
			timeStamp = timeStamp / Common.MINUTE_FORMATER
					* Common.MINUTE_FORMATER;
			byte[] arr = CellUtil.cloneValue(cell);
			int rssi = -100;
			if (arr.length == 2) {
				rssi = Bytes.toShort(arr);
			}
			else {
				rssi = Bytes.toInt(arr);
			}
			
			if (!timeRssiMap.containsKey(timeStamp)) {
				timeRssiMap.put(timeStamp, rssi);
			}
			else {
				int rssiMax = timeRssiMap.get(timeStamp);
				//get the max rssi signal
				if (rssi > rssiMax) {
					timeRssiMap.put(timeStamp, rssi);
				}
			}		
		}
		
		Iterator<Long> iter = timeRssiMap.keySet().iterator();
		while (iter.hasNext()) {
			long timeStamp = iter.next();
			cb.addTimeStamp(timeStamp);
			cb.addApRssi(timeRssiMap.get(timeStamp));
		}
		sb.addCustomer(cb);

		LongWritable outKey = new LongWritable(sb.getShopId());
		BytesWritable outVal = new BytesWritable(sb.build().toByteArray());
		context.write(outKey, outVal);
	}

	private String getMacFromArr(byte[] arr, int start, int end) {
		StringBuffer mac = new StringBuffer();
		for (int i = start; i < end; i++) {
			mac.append(String.format(Common.MAC_FORMAT, arr[i]));
			mac.append(':');
		}
		mac.deleteCharAt(mac.length() - 1);
		return mac.toString().toLowerCase();
	}

}
