package com.doodod.market.analyse;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.lang.Math;

import com.doodod.market.message.Store.Customer;
import com.doodod.market.message.Store.Shop;
import com.doodod.market.message.Store.UserType;

import Jama.Matrix;


public class UserClassify {
	
	final static long MINUTE = 60000;  // one minute is 60000 ms
	final static long HOUR = 60*MINUTE;  
	final static long MIN_PASSENGER_TIME =  30*MINUTE;
	final static long MAX_PASSENGER_TIME =  4*HOUR;	
	final static double[][] unitMatrixArrAy = {{1,0},{0,1}};
	final static Matrix UNIT_MATRIX2 = new Matrix(unitMatrixArrAy);
	
	private final String[] featureSelect = {"0","0 1","0 2"}; 

	private int classFlag;  // the method of classifying
	
	public UserClassify(){}	
	public UserClassify(Shop shop){
		classFlag = 2;
		train(shop);
	}
	public UserClassify(Shop shop,int flag){
		classFlag = flag;
		train(shop);
	}
	//
	//get the UserType of the parameter
	//
	public static UserType getUserType(Customer cus){
		int stampCount = cus.getTimeStampCount();
		
		if(stampCount<=1) return UserType.PASSENGER;  //there is only one time stamp	
		
		//sort
		long[] timeStampArry = new long[stampCount];
		for(int j = 0;j<stampCount;j++){
			timeStampArry[j] = cus.getTimeStamp(j);
		}
		Arrays.sort(timeStampArry);  
		
		//compute the time delta
		long[] timeDelta = new long[stampCount-1];
		for(int j = 0;j<stampCount-1;j++){
			timeDelta[j] = timeStampArry[j+1]-timeStampArry[j];
		}
		
		//make decision
		if(myMax(timeDelta) > MAX_PASSENGER_TIME)  return UserType.PASSENGER;
			
		return UserType.CUSTOMER;
	} 
	//
	//train the Bayes Machine 
	//	 
	private List<Float> priProb = new ArrayList<Float>();  // the prior probability
	private List<GaussPara> GaussPara = new ArrayList<GaussPara>(); // the Guass parameters of 4 modules in the third method
	
	public boolean train(Shop shop){
		return train(shop,classFlag);
	}
	public boolean train(Shop shop,int flag){
		if(flag>=featureSelect.length)
			return false;
		
		//get feature index according to classFlag
		String arr[] = featureSelect[flag].split(" ");
		int featureIdx[] = new int[arr.length];
		for(int i=0;i<arr.length;i++)
			featureIdx[i] = Integer.parseInt(arr[i]);
		
		//clear
		classFlag = flag;
		priProb.clear();
		GaussPara.clear();
		for(int i=0;i<4;i++){
			priProb.add((float) 0);
			GaussPara.add(new GaussPara(arr.length));
		}
		
		//get samples according to classFlag
		List<Sample> sampleList = new ArrayList<Sample>();
		for(Customer cus : shop.getCustomerList()){
			Matrix allFeatures = getAllFeatures(cus);
			Matrix features = new Matrix(1,arr.length,(float)0);
			for(int i=0;i<arr.length;i++)
				features.set(0, i, allFeatures.get(0, featureIdx[i]));
			int type = cus.getUserType().getNumber();
			sampleList.add(new Sample(features,type));
			priProb.set(type, priProb.get(type)+1);
		}
		
		//compute parameters of 4 Gauss modules
		for(Sample sample : sampleList){
			GaussPara.get(sample.type).mu = GaussPara.get(sample.type).mu.plus(sample.features);
		}
		for(int i=0;i<4;i++){
			GaussPara.get(i).mu = GaussPara.get(i).mu.times(1/priProb.get(i));
		}
		for(Sample sample : sampleList){
			Matrix delta = sample.features.minus(GaussPara.get(sample.type).mu);
			GaussPara.get(sample.type).sigma = GaussPara.get(sample.type).sigma.plus(delta.transpose().times(delta));
		}
		for(int i=0;i<4;i++){
			GaussPara.get(i).sigma = GaussPara.get(i).sigma.times(1/priProb.get(i));
		}	
		
		//compute prior probability
		for(int i=0;i<4;i++){
			priProb.set(i, priProb.get(i)/shop.getCustomerCount());
		}
			
		return true;
	}

	//
	//test the Bayes Machine 
	//		
	public BayesResult test(Customer cus){
		return test(cus,classFlag);
	}
	public BayesResult test(Customer cus,int flag){
		classFlag = flag;
		List<Float> prob = new ArrayList<Float>();
		for(int i=0;i<4;i++){
			prob.add((float) 0);
		}
		if(cus.getTimeStampCount()==1){
			prob.set(UserType.PASSENGER.getNumber(), (float) 1);
			BayesResult res = new BayesResult(UserType.PASSENGER,prob);
			return res;
		}
		Matrix features = getFeatures(cus);
		for(int i=0;i<4;i++){
			prob.set(i, priProb.get(i)*GaussProb(features,GaussPara.get(i)));
		}
		int maxIdx = 0;
		float maxValue = prob.get(maxIdx);
		for(int i=1;i<4;i++){
			if(prob.get(i)>maxValue){
				maxIdx = i;
				maxValue = prob.get(i);
			}
		}		
		BayesResult res = new BayesResult(UserType.valueOf(maxIdx),prob);
		return res;
	}
	//
	//compute the standing time(minute) of a person per day
	//
	public long getStandingTime(Customer cus){
		List<TimeStampCluster> clusterList = HieCluster(cus);
		long count = 0;
		for(TimeStampCluster cluster : clusterList){
			count += (cluster.list.get(cluster.list.size()-1)-cluster.list.get(0)+1*MINUTE);
		}
		count = count/MINUTE;
		return count;
		
	}
	//
	//compute the Gauss probability
	//
	private float GaussProb(float x,float mu,float var){
		if(var==0){
			//return (x==mu) ? 9999 : 0;
			var = (float) 0.001;
		}
		return (float) (1/Math.sqrt(2*Math.PI*var)*Math.exp(-0.5*Math.pow(x-mu, 2)/var));
	}
	private float GaussProb(Matrix x,GaussPara para){
		int n = para.dimension;
		while(para.sigma.det()==0)
			for(int i=0;i<n;i++)
				para.sigma.set(i, i, para.sigma.get(i, i)+0.1);
		return (float) ((float)1/Math.sqrt(Math.pow(2*Math.PI,n)*para.sigma.det())*Math.exp(x.minus(para.mu).times(para.sigma.inverse()).times(x.minus(para.mu).transpose()).times(-0.5).det()));
	}
	//
	//compute the Poisson probability
	//
	private float PoissonProb(int k,float lamda){
		return (float) ((float) Math.exp(-lamda)*Math.pow(lamda, k)/Factorial(k));
	}
	//
	//compute n!
	//
	private int Factorial(int n){
		if(n<0) return -1;
		if(n==0 || n==1) return 1;
		return n*Factorial(n-1);
	}
	class BayesResult{
		public UserType result;
		public List<Float> probability = new ArrayList<Float>();
		public BayesResult(UserType res,List<Float> prob){
			result = res;
			probability = prob;
			}
		}
		
	
	//
	// Gauss parameter class
	//
	class GaussPara{
		int dimension; 
		Matrix mu;
		Matrix sigma;
		public GaussPara(Matrix m,Matrix s){
			dimension = m.getColumnDimension();
			mu = m;
			sigma = s;
		}
		public GaussPara(int dim) {
			dimension = dim;
			mu = new Matrix(1, dim, 0);
			sigma = new Matrix(dim,dim,0);
		}
		public void set(Matrix m,Matrix s){
			dimension = m.getColumnDimension();
			mu = m;
			sigma = s;			
		}
	}
	//
	// sample class
	//
	class Sample{
		int dimension;
		Matrix features;
		int type;
		public Sample(Matrix f,int t){
			dimension = f.getColumnDimension();
			features = f;
			type = t;
		}
	}
	//
	// one timeStamp cluster of a customer
	//
	class TimeStampCluster{  
		List<Long> list = new ArrayList<Long>();
		public float var; //variance
		public long distance;  // the distance with next TimeStampCluster
		public TimeStampCluster(long first,long dis){			
			list.add(first);
			distance = dis;
			var = 0;
		}
		private void updateVar(){
			float average = 0;
			for(int i=0;i<list.size();i++){
				average += (float)list.get(i);
			}
			average /= list.size();
			float diff = 0;
			for(int i=0;i<list.size();i++){
				diff += Math.pow(list.get(i)-average, 2);
			}
			diff /= list.size();
			var = diff;
		}
		private int getSize(){return list.size();}
		private void setDistance(long dis){
			distance = dis;
		}
	}
	//
	// hierarchical clustering
	//
	//
	private List<TimeStampCluster> HieCluster(Customer cus){
		List<TimeStampCluster> clusterList = new ArrayList<TimeStampCluster>();
		if(cus.getTimeStampCount()==1){
			clusterList.add(new TimeStampCluster(cus.getTimeStamp(0),0));
			return clusterList;
		}
		
		//hierarchical cluster
		int i;
		for(i=0;i<cus.getTimeStampCount()-1;i++){   //initialize
			clusterList.add(new TimeStampCluster(cus.getTimeStamp(i),cus.getTimeStamp(i+1)-cus.getTimeStamp(i)));
		}
		clusterList.add(new TimeStampCluster(cus.getTimeStamp(i),0));
		while(clusterList.size()>1){  // start cluster
			// seek the minimum of distance between clusters
			int minIdx = 0;
			long minDis = clusterList.get(minIdx).distance;
			if(clusterList.size()>2)
				for(i=1;i<clusterList.size()-1;i++)
					if(clusterList.get(i).distance<minDis){
						minDis = clusterList.get(i).distance;
						minIdx = i;
					}
			if(minDis>MIN_PASSENGER_TIME) break;
			// conbine two cluster into one cluster
			clusterList.get(minIdx).list.addAll(clusterList.get(minIdx+1).list);
			clusterList.remove(minIdx+1);
			//update variance and distance
			//clusterList.get(minIdx).updateVar();
			if(minIdx<clusterList.size()-1)
				clusterList.get(minIdx).setDistance(clusterList.get(minIdx+1).distance-clusterList.get(minIdx).distance);
		}
		return clusterList;
	}
	//
	// get all features of a customer 
	//
	private Matrix getAllFeatures(Customer cus){
		Matrix features = new Matrix(1,3,(float)0);  
		if(cus.getTimeStampCount()==1){  //there is only one stamp
			features.set(0, 0, 1);  //feature 1 : count of all time stamps 
			features.set(0, 1, 0);  //feature 2 : variance of rssi 
			features.set(0, 2, 1);  //feature 3 : maximum of stamp count of each group divided from time stamps
		}		
		else{   // there are many stamps
			
			//feature 1 : count of all time stamps 
			features.set(0, 0, cus.getTimeStampCount());
			
			//feature 2 : variance of rssi 
			float mean = 0;
			for(int temp : cus.getApRssiList()){
				mean += temp;
			}
			mean /= cus.getApRssiCount();
			float var = 0;
			for(int temp : cus.getApRssiList()){
				var += Math.pow(temp-mean, 2);
			}
			var /= cus.getApRssiCount();
			features.set(0, 1, var);
			
			//feature 3 : max of stamp count in groups divided from time stamps
			List<TimeStampCluster> clusterList = HieCluster(cus);
			float maxSize = 0;
			for(TimeStampCluster temp : clusterList){
				maxSize = Math.max(maxSize, temp.getSize());
			}
			features.set(0, 2, maxSize);
		}
		return features;
	}
	//
	//get specific features from all features according to classFlag
	//
	private Matrix getFeatures(Customer cus){
		return getFeatures(getAllFeatures(cus));
	}
	private Matrix getFeatures(Matrix allFeatures){
		return getFeatures(allFeatures,classFlag);
	}
	private Matrix getFeatures(Matrix allFeatures,int flag){
		classFlag = flag;
		String arr[] = featureSelect[flag].split(" ");
		int featureIdx[] = new int[arr.length];
		for(int i=0;i<arr.length;i++)
			featureIdx[i] = Integer.parseInt(arr[i]);
		Matrix features = new Matrix(1,arr.length,(float)0);
		for(int i=0;i<arr.length;i++)
			features.set(0, i, allFeatures.get(0, featureIdx[i]));
		return features;
	}
	private static long myMin(long[] data){   //get the minimum value in a arry
		if(data.length==1) return data[0];
		long minValue = data[0];
		for(int i=1;i<data.length;i++){
			minValue = Math.min(minValue, data[i]);
		}
		return minValue;
	}
	private static ValueAndIdx myMin2(long[] data){   //get the minimum value and idx in a arry
		if(data.length==1) 
			return (new ValueAndIdx(data[0],0));
		long minValue = data[0];
		int minIdx = 0;
		for(int i=1;i<data.length;i++){
			if(data[i]<minValue){ 
				minValue = data[i];
				minIdx = i;
			} 
		}
		return (new ValueAndIdx(minValue,minIdx));
	}	
	static class ValueAndIdx{  //the pattern of myMin2's return
		long value;
		int idx;
		public ValueAndIdx(long v,int i){
			value = v;
			idx = i;
		}
		public long getValue(){return value;}
		public int getIdx(){return idx;}
	}   
	private static long myMax(long[] data){   //get the minimum value in a arry
		if(data.length==1) return data[0];
		long minValue = data[0];
		for(int i=1;i<data.length;i++){
			minValue = Math.max(minValue, data[i]);
		}
		return minValue;
	}

	
}
