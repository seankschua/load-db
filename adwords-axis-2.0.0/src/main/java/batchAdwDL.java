import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.google.api.ads.adwords.lib.client.AdWordsSession;
import com.google.api.ads.common.lib.auth.OfflineCredentials;
import com.google.api.ads.common.lib.auth.OfflineCredentials.Api;
import com.google.api.ads.common.lib.conf.ConfigurationLoadException;
import com.google.api.ads.common.lib.exception.OAuthException;
import com.google.api.ads.common.lib.exception.ValidationException;
import com.google.api.client.auth.oauth2.Credential;

public class batchAdwDL {

	public static final String directory = "C:\\Users\\schuakianshun\\workspace\\adwords-axis-2.0.0\\src\\main\\resources";
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		ArrayList<String> reportList = new ArrayList<String>();
		ArrayList<String> clientList = new ArrayList<String>();
		ArrayList<String> yearList = new ArrayList<String>();
		ArrayList<String> monthList = new ArrayList<String>();
		
		reportList.add("sq");
		reportList.add("kw");
		
		clientList.add("625-898-2657");
		
		clientList.add("650-647-5316");
		clientList.add("205-085-2785");
		clientList.add("614-715-1681");
		clientList.add("755-787-7834");
		clientList.add("750-364-8530");
		clientList.add("494-728-9561");
		clientList.add("186-235-1155");
		clientList.add("107-419-4775");
		clientList.add("536-619-7879");
		clientList.add("132-066-5226");
		clientList.add("878-409-7667");
		clientList.add("785-989-6624");
		clientList.add("304-175-4936");
		clientList.add("652-642-7655");
		
		yearList.add("2015");
		
		monthList.add("1");
		monthList.add("2");
		monthList.add("3");
		monthList.add("4");
		monthList.add("5");
		monthList.add("6");
		monthList.add("7");
		monthList.add("8");
		monthList.add("9");
		//monthList.add("10");
		//monthList.add("11");
		//monthList.add("12");
		
		
		for (String report:reportList){
			System.out.println("Downloading report type " + report + "...");
			for (String client:clientList){
				System.out.println("Downloading report for " + client + "...");
				for (String year:yearList){
					System.out.println("Downloading report for Year: " + year + "...");
					for (String month:monthList){
						System.out.println("Downloading report for Month: " + month + "...");
						
						String[] args2Insert = new String[4];
						args2Insert[0] = report;
						args2Insert[1] = client;
						args2Insert[2] = year;
						args2Insert[3] = month;
						
						//System.out.println(Arrays.toString(args2Insert));
								
						try {
							DownloadReportWithAwql.main(args2Insert);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}			
		}
		
		
		
		
	}

}
