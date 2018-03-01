package com.doodod.market.apply;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.doodod.market.message.Store.Shop;
import com.doodod.market.statistic.Common;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class CalculateVisitForHistoryMapper extends
		TableMapper<Text, Text> {
	enum JobCounter {
		DB_APMAC_NULL, 
		DB_APSHOPID_NULL, 
		BRAND_MAP_ERROR, 
		AP_NOT_IN_MAP, 
		MAP_TOTAL
	}

	private static Map<String, Integer> apShopMap = new HashMap<String, Integer>();

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		Shop.Builder sb = Shop.newBuilder();

		byte[] macArr = key.get();
		String apMac = getMacFromArr(macArr, 0, Common.AP_MAC_LENGTH);
		String phoneMac = getMacFromArr(macArr, Common.AP_MAC_LENGTH,
				macArr.length);

		String phoneMacKey = phoneMac.substring(0, Common.MAC_KEY_LENGTH);

		context.getCounter(JobCounter.MAP_TOTAL).increment(1);
		if (!apShopMap.containsKey(apMac)) {
			context.getCounter(JobCounter.AP_NOT_IN_MAP).increment(1);
			return;
		}
		int shopId = apShopMap.get(apMac);

		if(value.listCells().isEmpty())
		{
			return;
		}
		long timeStamp = value.listCells().get(0).getTimestamp();
		
		context.write(new Text(phoneMac),new Text(timeStamp + Common.CTRL_A + shopId));

	}

	@Override
	protected void setup(Context context) throws IOException,
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
