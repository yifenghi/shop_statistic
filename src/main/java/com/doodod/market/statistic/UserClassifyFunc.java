package com.doodod.market.statistic;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Set;

import com.doodod.market.analyse.UserClassifyNew;
import com.doodod.market.message.Store.Customer;
import com.doodod.market.message.Store.UserType;
import com.doodod.market.statistic.ShopReducer.JobCounter;

public class UserClassifyFunc {	
	
	public static void getClassifyMartix(String filePath, int featureSize,
 			Map<Integer, UserClassifyNew> shopModelMap) throws Exception {
		if (featureSize == 0) {
			return;
		}
		
		Charset charSet =  Charset.forName("UTF-8");
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				new FileInputStream(filePath), charSet));
		String line = "";
		while ((line = reader.readLine()) != null) {
			String arrKV[] = line.split(Common.TAB, -1);
			
			if (arrKV.length != 2) {
				continue;
			}
			int shopId = Integer.parseInt(arrKV[0]);
			
			String arrModel[] = arrKV[1].split(Common.CTRL_A, -1);
			if (arrModel.length != 2) {
				continue;
			}
			
			String arrPass[] = arrModel[0].split(Common.CTRL_B, -1);
			float priProbPass = Float.parseFloat(arrPass[0]);
			double muPass[][] = new double[1][featureSize];
			double sigmPass[][] = new double[featureSize][featureSize];

			getMatrix(arrPass[1], muPass);
			getMatrix(arrPass[2], sigmPass);
			
			String arrCust[] = arrModel[1].split(Common.CTRL_B, -1);
			float priProbCust = Float.parseFloat(arrCust[0]);
			
			double muCust[][] = new double[1][featureSize];
			double sigmCust[][] = new double[featureSize][featureSize];

			getMatrix(arrCust[1], muCust);
			getMatrix(arrCust[2], sigmCust);

			UserClassifyNew classify = new UserClassifyNew(
					priProbPass, priProbCust, muPass, sigmPass, muCust, sigmCust);
			shopModelMap.put(shopId, classify);
		}
		reader.close();
	}
	
	private static void getMatrix(String str, double[][] arr) {
		String arrMatrix[] = str.split(Common.CTRL_C, -1);
		for (int i = 0; i < arrMatrix.length; i++) {
			String arrVal[] = arrMatrix[i].split(Common.CTRL_D, -1);
			for (int j = 0; j < arrVal.length; j++) {
				arr[i][j] = Double.parseDouble(arrVal[j]);
			}
		}
	}

}
