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

	//public static final String directory = "C:\\Users\\schuakianshun\\workspace\\adwords-axis-2.0.0\\src\\main\\resources";
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		int counter = 0;
		
		ArrayList<String> reportList = new ArrayList<String>();
		
		//ArrayList<String> clientList = new ArrayList<String>(Arrays.asList("1074194775"));
		ArrayList<String> clientList = Utility.accounts;
		
		ArrayList<String> yearList = new ArrayList<String>(Arrays.asList("2015"));
		
		//ArrayList<String> monthList = new ArrayList<String>(Arrays.asList("1"));
		ArrayList<String> monthList = Utility.monthList;
		
		//reportList.add("sq");
		//reportList.add("kw");
		//reportList.add("adg");
		//reportList.add("ad");
		//reportList.add("cam_names");
		//reportList.add("adg_names");
		//reportList.add("ad_names");
		//reportList.add("kw0");
		//reportList.add("sq0");
		
		//date agnostic, but sets query range to jan1 of year to 2 days before today
		reportList.add("adgn");
		
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
						} finally {
							counter++;
						}
					}
				}
			}			
		}
		
		
		
		System.out.println("batchAdwDL() completed with " + counter + " files.");
	}

}
