/**
 * @author caoyupeng@doodod.com
 */
package com.doodod.market.statistic;

public class Common {
	public static final char MERGE_TAG_P = 'P';
	public static final char MERGE_TAG_T = 'T';
	public static final char FLOW_TAG_M = 'M';
	public static final char FLOW_TAG_H = 'H';
	public static final char FLOW_TAG_D = 'D';

	public static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	public static final String DATE_FORMAT = "yyyy-MM-dd";
	public static final String NUM_FORNAT  = "###0.0000";
	public static final String MAC_FORMAT  = "%02x";
	public static final String HBASE_RSSI = "RSSI";
	public static final String COMMA      = ",";
	public static final String UNDER_LINE = "_";
	public static final String NULL       = "NULL";
	public static final String CTRL_A = "\u0001";
	public static final String CTRL_B = "\u0002";
	public static final String CTRL_C = "\u0003";
	public static final String CTRL_D = "\u0004";
	public static final String TAB    = "\t";

	public static final String MONGO_OPTION_SET = "$set";

	public static final String BIZDATE = "doodod.system.bizdate";
	public static final String MONGO_SERVER_LIST = "mongo.server.list";
	public static final String MONGO_SERVER_PORT = "mongo.server.port";
	public static final String MONGO_DB_NAME = "mongo.db.singlestore";
	
	public static final String MONGO_COLLECTION_APMAC        = "mongo.collection.apmac";
	public static final String MONGO_COLLECTION_APMAC_MAC    = "mongo.collection.apmac.mac";
	public static final String MONGO_COLLECTION_APMAC_SHOPID = "mongo.collection.apmac.shopid";
	public static final String MONGO_COLLECTION_APMAC_STATUS = "mongo.collection.apmac.status";
	
	public static final String MONGO_COLLECTION_BUSINESSTIME           = "mongo.collection.businesstime";
	public static final String MONGO_COLLECTION_BUSINESSTIME_STRATTIME = "mongo.collection.businesstime.starttime";
	public static final String MONGO_COLLECTION_BUSINESSTIME_ENDTIME   = "mongo.collection.businesstime.endtime";
	public static final String MONGO_COLLECTION_BUSINESSTIME_SHOPID    = "mongo.collection.businesstime.shopid";
	
	public static final String MONGO_COLLECTION_USERINFO    = "mongo.collection.userinfo";
	public static final String MONGO_COLLECTION_USER_SHOPID = "mongo.collection.user.shopid";
	public static final String MONGO_COLLECTION_USER_TYPE   = "mongo.collection.user.type";
	public static final String MONGO_COLLECTION_USER_MAC    = "mongo.collection.user.mac";
	public static final String MONGO_COLLECTION_USER_TIME   = "mongo.collection.user.time";
	
	public static final String MONGO_COLLECTION_MALLINFO     = "mongo.collection.mallinfo";
	public static final String MONGO_COLLECTION_MALL_TIME    = "mongo.collection.mall.time";
	public static final String MONGO_COLLECTION_MALL_SHOPID  = "mongo.collection.mall.shopid";
	
	public static final String MONGO_COLLECTION_VISITINFO   = "mongo.collection.visitinfo";
	public static final String MONGO_COLLECTION_VISIT_OLD   = "mongo.collection.visit.old";
	public static final String MONGO_COLLECTION_VISIT_NEW   = "mongo.collection.visit.new";

	public static final String MONGO_COLLECTION_CUSTOMERINFO     = "mongo.collection.customerinfo";
	public static final String MONGO_COLLECTION_CUSTOMER_SHOPID  = "mongo.collection.customer.shopid";
	public static final String MONGO_COLLECTION_CUSTOMER_UPDATE  = "mongo.collection.customer.update";
	public static final String MONGO_COLLECTION_CUSTOMER_CREATE  = "mongo.collection.customer.create";
	public static final String MONGO_COLLECTION_CUSTOMER_DWELL   = "mongo.collection.customer.dwell";
	public static final String MONGO_COLLECTION_CUSTOMER_USERMAC = "mongo.collection.customer.usermac";
	public static final String MONGO_COLLECTION_CUSTOMER_TIMESATMPS = "mongo.collection.customer.timestamps";
	public static final String MONGO_COLLECTION_CUSTOMER_TYPE    = "mongo.collection.customer.type";
	
	public static final String MONGO_COLLECTION_COUNT           = "mongo.collection.count";
	public static final String MONGO_COLLECTION_COUNT_ID        = "mongo.collection.count.id";
	public static final String MONGO_COLLECTION_COUNT_CUSTOMER  = "mongo.collection.count.customer";
	public static final String MONGO_COLLECTION_COUNT_EMPLOYEE  = "mongo.collection.count.employee";
	public static final String MONGO_COLLECTION_COUNT_MACHINE   = "mongo.collection.count.machine";
	public static final String MONGO_COLLECTION_COUNT_PASSENGER = "mongo.collection.count.passenger";
	public static final String MONGO_COLLECTION_COUNT_TOTAL     = "mongo.collection.count.total";
	public static final String MONGO_COLLECTION_COUNT_TIME      = "mongo.collection.count.time";

	
	public static final String MERGE_INPUT_PART  = "merge.input.part";
	public static final String MERGE_INPUT_TOTAL = "merge.input.total";
	
	
	public static final String USERTYPE_MACHINE  = "0";
	public static final String USERTYPE_EMPLOYEE = "1";
	
	public static final String USERTYPE_TAG_CUSTOMER  = "user.type.tag.customer";
	public static final String USERTYPE_TAG_PASSENGER = "user.type.tag.passenger";
	public static final String USERTYPE_TAG_EMPLOYEE  = "user.type.tag.employee";
	public static final String USERTYPE_TAG_MACHINE   = "user.type.tag.machine";
	public static final String USERTYPE_TAG_TOTAL     = "user.type.tag.total";
	public static final String MALL_INFO_TIMESTAMP    = "mall.info.time.stamp";
	
	public static final String FLOW_INPUT_MIN  = "flow.input.min";
	public static final String FLOW_INPUT_HOUR = "flow.input.hour";
	public static final String FLOW_INPUT_DAY  = "flow.input.day";
	public static final String TIME_TAG_MIN   = "time.tag.min";
	public static final String TIME_TAG_HOUR  = "time.tag.hour";

	public static final String CUSTOMER_TIME_CREATE = "customer.time.create";
	public static final String CUSTOMER_TIME_UPDATE = "customer.time.update";
	
	public static final String BUSINESSTIME_START = "store.businesstime.start";
	public static final String BUSINESSTIME_NOW   = "store.businesstime.now";
	public static final String BUSINESSTIME_OUT   = "businesstime";
	//add by lifeng
	public static final String BUSINESSTIME_END = "store.businesstime.end";
	
	

	public static final String TRAIN_PASSENGER_FEATURELIST    = "train.passenger.featurelist";
	public static final String TRAIN_PASSENGER_DEFAULT_FILTER = "train.passenger.default.filter";
	public static final String TRAIN_MODEL_PATH               = "train.model.path";

	public static final String BRAND_MAP_PATH     = "brand.map.path";
	public static final String BRAND_NAME_UNKNOWN = "unknown";
	
	public static final String MACHINE_TIME_FILTER   = "machine.time.filter";
	public static final String MACHINE_COUNT_FILTER  = "machine.count.filter";
	public static final String MACHINE_BRAND_PATH    = "machine.brand.path";
	public static final String EMPLOYEE_TIME_FILTER  = "employee.time.filter";
	public static final String EMPLOYEE_COUNT_FILTER = "employee.count.filter";
	public static final String RSSI_INDOOR_SCOPE     = "rssi.indoor.scope";
	public static final String RSSI_MACHINE_VAR      = "rssi.machine.var";

	
	public static final int MONGO_SERVER_NUM = 3;
	public static final int AP_MAC_LENGTH    = 6;
	public static final int MINUTE_FORMATER  = 60 * 1000;
	public static final int AP_STATUS_FAIL = 1;
	public static final int AP_STATUS_OK   = 0;
	public static final int MAC_KEY_LENGTH = 8;
	
	public static final int DEFAULT_INDOOR_SCOPE = -75;
	public static final int DEFAULT_PASSENGER_FILTER = 3;
	
	//add by lifeng

	public static final String CONF_BRAND_LIST    = "conf.brand.list";
	public static final String DEFAULT_MAC_BRAND  = "其他";	
	public static final String MONGO_COLLECTION_BRAND       = "mongo.collection.brand";
	public static final String MONGO_COLLECTION_BRAND_NAME  = "mongo.collection.brand.name";
	public static final String MONGO_COLLECTION_BRAND_COUNT = "mongo.collection.brand.count";
	public static final String MONGO_COLLECTION_BRAND_TIME  = "mongo.collection.brand.time";
	public static final int DEFAULT_MONGO_PORT = 27017;
	

	public static final String MARKET_SYSTEM_TODAY   = "market.system.today";

}
