package com.doodod.market.analyse;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.doodod.market.message.Store.Customer;
import com.doodod.market.message.Store.Shop;
import com.doodod.market.message.Store.UserType;
import com.doodod.market.statistic.Common;

import Jama.Matrix;

public class UserClassifyNew {

	private float priProbCustomer;
	private float priProbPassenger;
	private float priProbInDoor;
	private float priProbOutDoor;

	private GaussPara gaussParaCustomer;
	private GaussPara gaussParaPassenger;
	private GaussPara gaussParaInDoor;
	private GaussPara gaussParaOutDoor;

	private final static long MINUTE = 60000; // one minute is 60000 ms
	private final static long MIN_PASSENGER_TIME = 30 * MINUTE;

	// feature 1 : count of all time stamps
	public static final int FEATURE_TIMENUM = 0;
	// feature 2 : variance of rssi
	public static final int FEATURE_RSSIVAR = 1;
	// feature 3 : maximum of stamp count of each group divided from time stamps
	public static final int FEATURE_TIMEMAX = 2;
	public static final int FEATURE_RSSIAVG = 3;
	public static final int FEATURE_COUNTMAX = 4;
	public static final int FEATURE_RSSIAVGMAX = 5;
	public static final int FEATURE_RSSIMAX = 6;
	public static final int FEATURE_TOTAL = 7;

	public static int Rssi_Indoor;

	public UserClassifyNew() {

	}

	public UserClassifyNew(float priPass, float priCust, double[][] arrMuPass,
			double[][] arrSigmPass, double[][] arrMuCust, double[][] arrSigmCust) {
		this.priProbPassenger = priPass;
		this.priProbCustomer = priCust;

		Matrix muPass = new Matrix(arrMuPass);
		Matrix sigmPass = new Matrix(arrSigmPass);
		Matrix muCust = new Matrix(arrMuCust);
		Matrix sigmCust = new Matrix(arrSigmCust);
		this.gaussParaPassenger = new GaussPara(muPass, sigmPass);
		this.gaussParaCustomer = new GaussPara(muCust, sigmCust);
	}

	public void trainPassenger(Shop shop, List<Integer> featureList) {
		priProbCustomer = 0;
		priProbPassenger = 0;
		List<Sample> sampleList = new ArrayList<Sample>();
		for (Customer cus : shop.getCustomerList()) {
			if (cus.getUserType() == UserType.MACHINE
					|| cus.getUserType() == UserType.EMPLOYEE
					|| cus.getUserType() == UserType.OUTDOOR ) {
				continue;
			}
			// calculate all feratues
			Matrix allFeatures = getAllFeatures(cus, FEATURE_TOTAL);

			Matrix features = new Matrix(1, featureList.size(), (float) 0);
			for (int i = 0; i < featureList.size(); i++) {
				features.set(0, i, allFeatures.get(0, featureList.get(i)));
			}
			
			if (cus.getUserType() != UserType.PASSENGER) {
				sampleList.add(new Sample(features, UserType.CUSTOMER));
				priProbCustomer++;
			} else {
				sampleList.add(new Sample(features, UserType.PASSENGER));
				priProbPassenger++;
			}
		}

		gaussParaCustomer = new GaussPara(featureList.size());
		gaussParaPassenger = new GaussPara(featureList.size());

		// compute parameters of 4 Gauss modules
		for (Sample sample : sampleList) {
			if (sample.type != UserType.PASSENGER) {
				gaussParaCustomer.mu = gaussParaCustomer.mu
						.plus(sample.features);
			} else {
				gaussParaPassenger.mu = gaussParaPassenger.mu
						.plus(sample.features);
			}
		}
		gaussParaCustomer.mu = gaussParaCustomer.mu.times(1 / priProbCustomer);
		gaussParaPassenger.mu = gaussParaPassenger.mu
				.times(1 / priProbPassenger);

		for (Sample sample : sampleList) {
			if (sample.type != UserType.PASSENGER) {
				Matrix delta = sample.features.minus(gaussParaCustomer.mu);
				gaussParaCustomer.sigma = gaussParaCustomer.sigma.plus(delta
						.transpose().times(delta));
			} else {
				Matrix delta = sample.features.minus(gaussParaPassenger.mu);
				gaussParaPassenger.sigma = gaussParaPassenger.sigma.plus(delta
						.transpose().times(delta));
			}
		}
		gaussParaCustomer.sigma = gaussParaCustomer.sigma
				.times(1 / priProbCustomer);
		gaussParaPassenger.sigma = gaussParaPassenger.sigma
				.times(1 / priProbPassenger);

		// compute prior probability
		priProbCustomer = priProbCustomer / shop.getCustomerCount();
		priProbPassenger = priProbPassenger / shop.getCustomerCount();

	}

	public void trainOutDoor(Shop shop, List<Integer> featureList) {
		priProbInDoor = 0;
		priProbOutDoor = 0;

		List<Sample> sampleList = new ArrayList<Sample>();
		for (Customer cus : shop.getCustomerList()) {
			// calculate all feratues
			Matrix allFeatures = getAllFeatures(cus, FEATURE_TOTAL);

			Matrix features = new Matrix(1, featureList.size(), (float) 0);
			for (int i = 0; i < featureList.size(); i++) {
				features.set(0, i, allFeatures.get(0, featureList.get(i)));
			}

			if (cus.getUserType() != UserType.PASSENGER) {
				if (cus.getUserType() != UserType.OUTDOOR) {
					sampleList.add(new Sample(features, UserType.CUSTOMER));
					priProbInDoor++;
				} else {
					sampleList.add(new Sample(features, UserType.OUTDOOR));
					priProbOutDoor++;
				}
			}
		}

		gaussParaInDoor = new GaussPara(featureList.size());
		gaussParaOutDoor = new GaussPara(featureList.size());

		// compute parameters of 4 Gauss modules
		for (Sample sample : sampleList) {
			if (sample.type != UserType.OUTDOOR) {
				gaussParaInDoor.mu = gaussParaInDoor.mu.plus(sample.features);
			} else {
				gaussParaOutDoor.mu = gaussParaOutDoor.mu.plus(sample.features);
			}
		}
		gaussParaInDoor.mu = gaussParaInDoor.mu.times(1 / priProbInDoor);
		gaussParaOutDoor.mu = gaussParaOutDoor.mu.times(1 / priProbOutDoor);

		for (Sample sample : sampleList) {
			if (sample.type != UserType.OUTDOOR) {
				Matrix delta = sample.features.minus(gaussParaInDoor.mu);
				gaussParaInDoor.sigma = gaussParaInDoor.sigma.plus(delta
						.transpose().times(delta));
			} else {
				Matrix delta = sample.features.minus(gaussParaOutDoor.mu);
				gaussParaOutDoor.sigma = gaussParaOutDoor.sigma.plus(delta
						.transpose().times(delta));
			}
		}
		gaussParaInDoor.sigma = gaussParaInDoor.sigma.times(1 / priProbInDoor);
		gaussParaOutDoor.sigma = gaussParaOutDoor.sigma
				.times(1 / priProbOutDoor);

		// compute prior probability
		priProbInDoor = priProbInDoor / shop.getCustomerCount();
		priProbOutDoor = priProbPassenger / shop.getCustomerCount();
	}

	public String toString() {
		DecimalFormat numberFormat = new DecimalFormat("0.000000");
		StringBuffer sb = new StringBuffer();
		sb.append(numberFormat.format(priProbPassenger)).append(Common.CTRL_B);
		arrToString(sb, gaussParaPassenger.mu.getArray());
		arrToString(sb, gaussParaPassenger.sigma.getArray());
		sb.deleteCharAt(sb.length() - 1);
		sb.append(Common.CTRL_A);

		sb.append(numberFormat.format(priProbCustomer)).append(Common.CTRL_B);
		arrToString(sb, gaussParaCustomer.mu.getArray());
		arrToString(sb, gaussParaCustomer.sigma.getArray());
		sb.deleteCharAt(sb.length() - 1);
		return sb.toString();
	}

	private void arrToString(StringBuffer sb, double arr[][]) {
		DecimalFormat numberFormat = new DecimalFormat("0.000000");
		for (double[] ds : arr) {
			for (double d : ds) {
				sb.append(numberFormat.format(d)).append(Common.CTRL_D);
			}
			sb.deleteCharAt(sb.length() - 1);
			sb.append(Common.CTRL_C);
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append(Common.CTRL_B);
	}

	public BayesResult test(Customer cus, List<Integer> featureList) {

		float probCust = 0;
		float probPass = 0;

		if (cus.getTimeStampCount() == 1) {
			probPass = 1;
			BayesResult res = new BayesResult(UserType.PASSENGER, probCust,
					probPass);
			return res;
		}

		Matrix allFeatures = getAllFeatures(cus, FEATURE_TOTAL);
		Matrix features = new Matrix(1, featureList.size(), (float) 0);
		for (int i = 0; i < featureList.size(); i++) {
			features.set(0, i, allFeatures.get(0, featureList.get(i)));
		}

		probCust = priProbCustomer * GaussProb(features, gaussParaCustomer);
		probPass = priProbPassenger * GaussProb(features, gaussParaPassenger);

		if (probCust > probPass) {
			return new BayesResult(UserType.CUSTOMER, probCust, probPass);
		} else {
			return new BayesResult(UserType.PASSENGER, probCust, probPass);
		}
	}

	public UserType classifyPassenger(Customer cus, List<Integer> featureList) {

		float probCust = 0;
		float probPass = 0;

		if (cus.getTimeStampCount() == 1) {
			return UserType.PASSENGER;
		}

		Matrix allFeatures = getAllFeatures(cus, FEATURE_TOTAL);
		Matrix features = new Matrix(1, featureList.size(), (float) 0);
		for (int i = 0; i < featureList.size(); i++) {
			features.set(0, i, allFeatures.get(0, featureList.get(i)));
		}

		probCust = priProbCustomer * GaussProb(features, gaussParaCustomer);
		probPass = priProbPassenger * GaussProb(features, gaussParaPassenger);

		if (probCust > probPass) {
			return UserType.CUSTOMER;
		} else {
			return UserType.PASSENGER;
		}
	}

	public UserType classifyPassengerWithWeight(Customer cus,
			List<Integer> featureList, List<Double> featureWeight) {

		float probCust = 0;
		float probPass = 0;

		if (cus.getTimeStampCount() == 1) {
			return UserType.PASSENGER;
		}

		Matrix allFeatures = getAllFeatures(cus, FEATURE_TOTAL);
		Matrix features = new Matrix(1, featureList.size(), (float) 0);
		for (int i = 0; i < featureList.size(); i++) {
			features.set(0, i, allFeatures.get(0, featureList.get(i)));
		}

		probCust = priProbCustomer
				* GaussProbNaive(features, gaussParaCustomer, featureWeight);
		probPass = priProbPassenger
				* GaussProbNaive(features, gaussParaPassenger, featureWeight);

		// probCust = priProbCustomer * GaussProbWithWeight(features,
		// gaussParaCustomer,featureWeight);
		// probPass = priProbPassenger * GaussProbWithWeight(features,
		// gaussParaPassenger,featureWeight);

		if (probCust > probPass) {
			return UserType.CUSTOMER;
		} else {
			return UserType.PASSENGER;
		}
	}

	// get all features of a customer
	private Matrix getAllFeatures(Customer cus, int featureSize) {
		// Construct an m-by-n constant matrix.
		Matrix features = new Matrix(1, featureSize, (float) 0);
		if (cus.getTimeStampCount() == 1) {
			features.set(0, FEATURE_TIMENUM, 1);
			features.set(0, FEATURE_RSSIVAR, 0);
			features.set(0, FEATURE_TIMEMAX, 1);
			features.set(0, FEATURE_RSSIAVG, -100);
			features.set(0, FEATURE_COUNTMAX, 1);
			features.set(0, FEATURE_RSSIAVGMAX, -100);
			features.set(0, FEATURE_RSSIMAX, -100);
		} else {
			// feature 0 : count of all time stamps
			features.set(0, FEATURE_TIMENUM, cus.getTimeStampCount());

			// feature 1 and 3 : variance and verage of rssi
			float sum = 0;
			if (cus.getApRssiCount() != 0) {
				for (int rssi : cus.getApRssiList()) {
					sum += rssi;
				}
				float mean = sum / cus.getApRssiCount();
				features.set(0, FEATURE_RSSIAVG, mean);

				float var = 0;
				for (int rssi : cus.getApRssiList()) {
					var += Math.pow(rssi - mean, 2);
				}

				var /= cus.getApRssiCount();
				features.set(0, FEATURE_RSSIVAR, var);
			} else {
				features.set(0, FEATURE_RSSIVAR, 0);
				features.set(0, FEATURE_RSSIAVG, -100);
			}

			// feature 2 : max of stamp interval in groups
			// float maxSize = getMaxTimeClusterSize(cus);
			float maxSize = (float) getMaxTimeClusterZone(cus)
					/ Common.MINUTE_FORMATER;
			features.set(0, FEATURE_TIMEMAX, maxSize);

			// feature 4,5,6: max of stamp count in groups divided from time
			// stamps,min of RSSI average in groups
			CusClusters clusters = HieCluster(cus);
//			float maxCount = 0;
//			for (TimeStampCluster temp : clusters.timeStampClusterList) {
//				maxCount = Math.max(maxCount, temp.getSize());
//			}
//			features.set(0, FEATURE_COUNTMAX, maxCount);
			features.set(0, FEATURE_COUNTMAX, 1);
			float maxRssiAverage = -100;
			for (RSSICluster temp : clusters.rssiClusterList) {
				float average = 0;
				for (int rssi : temp.list) {
					average += rssi;
				}
				average /= temp.getSize();
				maxRssiAverage = Math.max(maxRssiAverage, average);
			}
			features.set(0, FEATURE_RSSIAVGMAX, maxRssiAverage);

			// feature 7: max of rssi
//			int maxRssi = -100;
//			for (int rssi : cus.getApRssiList()) {
//				maxRssi = Math.max(maxRssi, rssi);
//			}
//			features.set(0, FEATURE_RSSIMAX, maxRssi);
			features.set(0, FEATURE_RSSIMAX, -100);


		}
		return features;
	}

	public float getMaxTimeClusterSize(Customer cust) {
		List<TimeStampCluster> clusterList = HieCluster(cust).timeStampClusterList;
		float maxSize = 1;
		for (TimeStampCluster timeCluster : clusterList) {
			if (maxSize < timeCluster.getSize()) {
				maxSize = timeCluster.getSize();
			}
		}
		return maxSize;
	}

	public long getTotalTimeClusterZone(Customer cust) {
		List<TimeStampCluster> clusterList = HieCluster(cust).timeStampClusterList;
		long totalTimeZone = 0;

		for (int i = 0; i < clusterList.size(); i++) {
			TimeStampCluster timeCluster = clusterList.get(i);

			int size = timeCluster.list.size();
			long timeZone = timeCluster.list.get(size - 1)
					- timeCluster.list.get(0) + MINUTE * 1;

			totalTimeZone += timeZone;
		}

		return totalTimeZone;
	}

	public long getMaxTimeClusterZone(Customer cust) {
		List<TimeStampCluster> clusterList = HieCluster(cust).timeStampClusterList;
		long maxTimeZone = 0;

		for (int i = 0; i < clusterList.size(); i++) {
			TimeStampCluster timeCluster = clusterList.get(i);

			int size = timeCluster.list.size();
			long timeZone = timeCluster.list.get(size - 1)
					- timeCluster.list.get(0) + MINUTE * 1;

			if (maxTimeZone < timeZone) {
				maxTimeZone = timeZone;
			}
		}

		return maxTimeZone;
	}

	public long getMaxTimeClusterInterval(Customer cust) {
		List<TimeStampCluster> clusterList = HieCluster(cust).timeStampClusterList;
		long maxTimeZone = 0;
		if (clusterList.size() == 1) {
			return 0;
		}

		for (int i = 0; i < clusterList.size(); i++) {
			TimeStampCluster timeCluster = clusterList.get(i);

			int size = timeCluster.list.size();
			long timeZone = timeCluster.list.get(size - 1)
					- timeCluster.list.get(0) + MINUTE * 1;

			if (maxTimeZone < timeZone) {
				maxTimeZone = timeZone;
			}
		}

		return maxTimeZone;
	}

	// hierarchical clustering
	public CusClusters HieCluster(Customer cus) {
		List<TimeStampCluster> clusterList = new ArrayList<TimeStampCluster>();
		List<RSSICluster> rssiClusterList = new ArrayList<RSSICluster>();
		CusClusters result = new CusClusters(clusterList, rssiClusterList);
		if (cus.getTimeStampCount() == 1) {
			clusterList.add(new TimeStampCluster(cus.getTimeStamp(0), 0));
			rssiClusterList.add(new RSSICluster(cus.getApRssi(0)));
			return result;
		}

		// hierarchical cluster
		int i;
		for (i = 0; i < cus.getTimeStampCount() - 1; i++) { // initialize
			clusterList.add(new TimeStampCluster(cus.getTimeStamp(i), cus
					.getTimeStamp(i + 1) - cus.getTimeStamp(i)));
			rssiClusterList.add(new RSSICluster(cus.getApRssi(i)));
		}
		clusterList.add(new TimeStampCluster(cus.getTimeStamp(i), 1));
		rssiClusterList.add(new RSSICluster(cus.getApRssi(i)));
		while (clusterList.size() > 1) { // start cluster
			// seek the minimum of distance between clusters
			int minIdx = 0;
			long minDis = clusterList.get(minIdx).distance;
			if (clusterList.size() > 2)
				for (i = 1; i < clusterList.size() - 1; i++)
					if (clusterList.get(i).distance <= minDis) {
						minDis = clusterList.get(i).distance;
						minIdx = i;
					}

			if (minDis > MIN_PASSENGER_TIME)
				break;
			// conbine two cluster into one cluster
			clusterList.get(minIdx).list
					.addAll(clusterList.get(minIdx + 1).list);
			clusterList.remove(minIdx + 1);

			rssiClusterList.get(minIdx).list.addAll(rssiClusterList
					.get(minIdx + 1).list);
			rssiClusterList.remove(minIdx + 1);
			// update variance and distance
			// clusterList.get(minIdx).updateVar();
			// rssiClusterList.get(minIdx).updateVar();
			if (minIdx < clusterList.size() - 1) {
				TimeStampCluster current = clusterList.get(minIdx);
				TimeStampCluster next = clusterList.get(minIdx + 1);

				current.setDistance(next.list.get(0)
						- current.list.get(current.list.size() - 1));
			}
		}
		return result;
	}

	private float GaussProb(Matrix x, GaussPara para) { // muti-dimension
		int n = para.dimension;
		while (para.sigma.det() == 0)
			for (int i = 0; i < n; i++)
				para.sigma.set(i, i, para.sigma.get(i, i) + 0.1);
		return (float) ((float) 1
				/ Math.sqrt(Math.pow(2 * Math.PI, n) * para.sigma.det()) * Math
					.exp(x.minus(para.mu).times(para.sigma.inverse())
							.times(x.minus(para.mu).transpose()).times(-0.5)
							.det()));
	}

	private float GaussProb(double x, double mu, double var) { // one dimension
		if (var == 0.0)
			var = 0.001;
		return (float) (1 / Math.sqrt(2 * Math.PI * var) * Math.exp(-Math.pow(x
				- mu, 2)
				/ 2 / var));
	}

	private float GaussProbWithWeight(Matrix x, GaussPara para,
			List<Double> featureWeight) { // muti-dimension
		int n = para.dimension;
		for (int i = 0; i < n; i++) {
			para.sigma
					.set(i,
							i,
							para.sigma.get(i, i)
									/ Math.pow(featureWeight.get(i), 0.01));
		}
		while (para.sigma.det() == 0)
			for (int i = 0; i < n; i++)
				para.sigma.set(i, i, para.sigma.get(i, i) + 0.01);
		return (float) ((float) 1
				/ Math.sqrt(Math.pow(2 * Math.PI, n) * para.sigma.det()) * Math
					.exp(x.minus(para.mu).times(para.sigma.inverse())
							.times(x.minus(para.mu).transpose()).times(-0.5)
							.det()));
	}

	private float GaussProbNaive(Matrix x, GaussPara para,
			List<Double> featureWeight) {
		int n = para.dimension;
		float result = 1;
		for (int i = 0; i < n; i++) {
			result *= GaussProb(x.get(0, i), para.mu.get(0, i),
					para.sigma.get(i, i) / featureWeight.get(i));
		}
		return result;
	}

	public static void main(String[] args) throws IOException {
		String inputDir = "/Users/paul/Documents/code_dir/doodod/shopstatistic/data.nak/data_6_17";
		String inputDirTest = "/Users/paul/Documents/code_dir/doodod/shopstatistic/data.nak/data.6.18.test";

		// List<Integer> featureList = new ArrayList<Integer>();
		// Shop.Builder sb = Shop.newBuilder();
		// List<Shop> shopList =
		// com.doodod.market.analyse.Common.getShopList(inputDir);
		// sb.mergeFrom(shopList.get(0));
		// sb.mergeFrom(shopList.get(1));

		Shop shopTest = com.doodod.market.analyse.Common.getShopList(inputDir)
				.get(1);

		double muPass[][] = new double[1][1];
		double sigmPass[][] = new double[1][1];
		muPass[0][0] = 1.290121;
		sigmPass[0][0] = 0.205951;
		double muCust[][] = new double[1][1];
		double sigmCust[][] = new double[1][1];
		muCust[0][0] = 3.980922;
		sigmPass[0][0] = 0.883906;

		UserClassifyNew classify = new UserClassifyNew(0.796215f, 0.203785f,
				muPass, sigmPass, muCust, sigmCust);

		// get samples according to classFlag
		// featureList = new ArrayList<Integer>();
		// featureList.add(FEATURE_TIMENUM);
		// featureList.add(FEATURE_RSSIVAR);
		// featureList.add(FEATURE_TIMEMAX);
		// featureList.add(FEATURE_RSSIAVG);

		// classify.trainPassernger(sb.build(), featureList);
		// System.out.println(classify.toString());

		int counter = 0;
		SimpleDateFormat timeFormat = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");

		for (Customer cus : shopTest.getCustomerList()) {
			if (cus.getTimeStampCount() < 10 || cus.getTimeStampCount() > 30) {
				continue;
			}
			System.out.println(classify.HieCluster(cus).size());

			System.out.println(classify.getMaxTimeClusterZone(cus)
					/ Common.MINUTE_FORMATER);

			for (long time : cus.getTimeStampList()) {
				System.out.println(time);
				System.out.println(timeFormat.format(new Date(time)));
			}
			counter++;
			break;

			// System.out.println(classify.classifyPassenger(cus, featureList));
			// BayesResult res = classify.test(cus, featureList);
			// System.out.println(res.result);
			// System.out.println(cus.getUserType());
			//
			// System.out.println(res.probCustomer);
			// System.out.println(res.probPassenger);
			//

		}
	}

	public class GaussPara {
		int dimension;
		Matrix mu;
		Matrix sigma;

		public GaussPara(Matrix m, Matrix s) {
			dimension = m.getColumnDimension();
			mu = m;
			sigma = s;
		}

		public GaussPara(int dim) {
			dimension = dim;
			mu = new Matrix(1, dim, 0);
			sigma = new Matrix(dim, dim, 0);
		}

		public void set(Matrix m, Matrix s) {
			dimension = m.getColumnDimension();
			mu = m;
			sigma = s;
		}
	}

	// one timeStamp cluster of a customer
	class TimeStampCluster {
		List<Long> list = new ArrayList<Long>();
		public float var; // variance
		public long distance; // the distance with next TimeStampCluster

		public TimeStampCluster(long first, long dis) {
			list.add(first);
			distance = dis;
			var = 0;
		}

		public TimeStampCluster() {
		}

		private void updateVar() {
			float average = 0;
			for (int i = 0; i < list.size(); i++) {
				average += (float) list.get(i);
			}
			average /= list.size();
			float diff = 0;
			for (int i = 0; i < list.size(); i++) {
				diff += Math.pow(list.get(i) - average, 2);
			}
			diff /= list.size();
			var = diff;
		}

		private int getSize() {
			return list.size();
		}

		private void setDistance(long dis) {
			distance = dis;
		}
	}

	// one RSSI cluster of a customer
	class RSSICluster {
		List<Integer> list = new ArrayList<Integer>();
		public float var; // variance

		public RSSICluster(int first) {
			list.add(first);
			var = 0;
		}

		public RSSICluster() {
		}

		private void updateVar() {
			float average = 0;
			for (int i = 0; i < list.size(); i++) {
				average += (float) list.get(i);
			}
			average /= list.size();
			float diff = 0;
			for (int i = 0; i < list.size(); i++) {
				diff += Math.pow(list.get(i) - average, 2);
			}
			diff /= list.size();
			var = diff;
		}

		public int getSize() {
			return list.size();
		}
	}

	class CusClusters { // result of HieCluster (stamp clusters and RSSI
						// clusters)
		List<TimeStampCluster> timeStampClusterList = new ArrayList<TimeStampCluster>();
		List<RSSICluster> rssiClusterList = new ArrayList<RSSICluster>();

		public int size() {
			return timeStampClusterList.size();
		}

		public CusClusters() {
		}

		public CusClusters(List<TimeStampCluster> timeStamp,
				List<RSSICluster> rssi) {
			timeStampClusterList = timeStamp;
			rssiClusterList = rssi;
		}
	}

	class Sample {
		int dimension;
		Matrix features;
		UserType type;

		public Sample(Matrix f, UserType t) {
			dimension = f.getColumnDimension();
			features = f;
			type = t;
		}
	}

	class BayesResult {
		public UserType result;
		public float probCustomer;
		public float probPassenger;

		public BayesResult(UserType res, float cust, float pass) {
			result = res;
			probCustomer = cust / (cust + pass);
			probPassenger = pass / (cust + pass);
		}
	}

	public void PreProcess(Customer.Builder cb, List<Integer> featureList) {
		if (cb.getUserType() == UserType.MACHINE
				|| cb.getUserType() == UserType.EMPLOYEE)
			return;

		if (featureList.contains(FEATURE_COUNTMAX)) {
			List<TimeStampCluster> timeStampClusterList = HieCluster(cb.build()).timeStampClusterList;
			int maxCount = 0;
			for (TimeStampCluster temp : timeStampClusterList) {
				maxCount = Math.max(maxCount, temp.getSize());
			}
			if (maxCount < 4)
				cb.setUserType(UserType.PASSENGER);
		}

		if (featureList.contains(FEATURE_TIMEMAX)) {
			List<TimeStampCluster> timeStampClusterList = HieCluster(cb.build()).timeStampClusterList;
			long maxTime = 0;
			for (TimeStampCluster temp : timeStampClusterList) {
				maxTime = Math.max(maxTime, temp.list.get(temp.list.size() - 1)
						- temp.list.get(0));
			}
			if (maxTime / MINUTE < 25)
				cb.setUserType(UserType.PASSENGER);
		}

		if (featureList.contains(FEATURE_RSSIAVG)) {
			float ssMean = 0;
			for (int ss : cb.getApRssiList())
				ssMean += ss;
			ssMean /= cb.getApRssiCount();
			if (ssMean < Rssi_Indoor)
				cb.setUserType(UserType.OUTDOOR);
		}

		if (featureList.contains(FEATURE_RSSIAVGMAX)) {
			List<RSSICluster> rssiClusterList = HieCluster(cb.build()).rssiClusterList;
			float maxTime = 0;
			for (RSSICluster temp : rssiClusterList) {
				float ssMean = 0;
				for (int ss : temp.list)
					ssMean += ss;
				ssMean /= temp.getSize();
				maxTime = Math.max(maxTime, ssMean);
			}
			if (maxTime < Rssi_Indoor)
				cb.setUserType(UserType.OUTDOOR);
		}
	}
}
