// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import com.google.api.ads.adwords.lib.client.AdWordsSession;
import com.google.api.ads.adwords.lib.jaxb.v201502.DownloadFormat;
import com.google.api.ads.adwords.lib.utils.ReportDownloadResponse;
import com.google.api.ads.adwords.lib.utils.ReportDownloadResponseException;
import com.google.api.ads.adwords.lib.utils.v201502.ReportDownloader;
import com.google.api.ads.common.lib.auth.OfflineCredentials;
import com.google.api.ads.common.lib.auth.OfflineCredentials.Api;
import com.google.api.client.auth.oauth2.Credential;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;

/**
 * This example downloads a criteria performance report with AWQL.
 *
 * Credentials and properties in {@code fromFile()} are pulled from the
 * "ads.properties" file. See README for more info.
 *
 * @author Kevin Winter
 */
public class DownloadReportWithAwql {

  public static void main(String[] args) throws Exception {
	
	// Generate a refreshable OAuth2 credential similar to a ClientLogin token
	// and can be used in place of a service account.
	Credential oAuth2Credential = new OfflineCredentials.Builder()
	    .forApi(Api.ADWORDS)
	    .fromFile()
	    .build()
	    .generateCredential();
	  
	/*
    // Construct an AdWordsSession.
    AdWordsSession session = new AdWordsSession.Builder()
        .fromFile()
        .withOAuth2Credential(oAuth2Credential)
        .build();
    */
	  
	AdWordsSession session = new AdWordsSession.Builder()
			.withDeveloperToken("zJlSw8AFSq756PGAwh4npA")
			.withOAuth2Credential(oAuth2Credential)
			.withUserAgent("expedia:adwords:v1")
			.withClientCustomerId(args[1])
			.build();
    
    String queryType = args[0];
    String client = args[1].replace("-", "");
    int year = Integer.parseInt(args[2]);
    int month = Integer.parseInt(args[3]);
    
    String returnQuery = returnQueryString(queryType, year, month);

    // Location to download report to.
    //DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm");
    //String dateTime = LocalDateTime.now().format(formatter);
    
    String reportFile = System.getProperty("user.home") + File.separatorChar + "reports" + File.separatorChar + queryType + "-" + year + "-" + month + "-" + client + ".csv";

    runExample(session, reportFile, returnQuery);
  }

  public static void runExample(AdWordsSession session, String reportFile, String returnQuery) throws Exception {
	  
	

    try {
      // Set the property api.adwords.reportDownloadTimeout or call
      // ReportDownloader.setReportDownloadTimeout to set a timeout (in milliseconds)
      // for CONNECT and READ in report downloads.
    	
      ReportDownloadResponse response =
          new ReportDownloader(session).downloadReport(returnQuery, DownloadFormat.CSV);
      response.saveToFile(reportFile);
      
      System.out.printf("Report successfully downloaded to: %s%n", reportFile);
    } catch (ReportDownloadResponseException e) {
      System.out.printf("Report was not downloaded due to: %s%n", e);
    }
  }
  
  public static String returnQueryString(String queryType, int year, int month) {
	  
	  Calendar calendar = Calendar.getInstance();
	  calendar.set(Calendar.YEAR, year);
	  calendar.set(Calendar.MONTH, month-1);
	  calendar.set(Calendar.DATE, 1);
	  Date queryDate = calendar.getTime();  
	  DateFormat sdf = new SimpleDateFormat("yyyyMMdd");
	  String queryDateString = sdf.format(queryDate);
	  calendar.add(Calendar.MONTH, 1);
	  calendar.add(Calendar.DATE, -1);
	  queryDate = calendar.getTime();
	  String queryDateString2 = sdf.format(queryDate);
	  //System.out.println(queryType + ", " + queryDateString + ", " + queryDateString2);
	  
	  String query = null;
	  
	  switch(queryType){
	  	case "sq":
	  		query = "SELECT Date, CreativeId, Query, ExternalCustomerId, CampaignId, AdGroupId, KeywordId, " +
	  	    	    "KeywordTextMatchingQuery, MatchType, MatchTypeWithVariant, Impressions, Clicks, Ctr, ConvertedClicks, ConversionsManyPerClick, ClickConversionRate, ConversionValue, " +
	  	    	    "Cost, AverageCpc, CostPerConvertedClick, " +
	  	    	    "DestinationUrl, FinalUrl, Device " +
	  	    	    "FROM   SEARCH_QUERY_PERFORMANCE_REPORT " +
	  	    	    "WHERE  Clicks > 0 AND AdGroupStatus = 'ENABLED' " +
	  	    	    "DURING " + queryDateString + "," + queryDateString2;
	  		break;
	  	case "kw":
	  		query = "SELECT Date, Id, ExternalCustomerId, CampaignId, AdGroupId, " +
		    "Criteria, KeywordMatchType, Impressions, Clicks, Ctr, ConvertedClicks, ConversionsManyPerClick, ClickConversionRate, ConversionValue, " +
		    "Cost, AverageCpc, CpcBid, CostPerConvertedClick, " +
		    "Labels, BiddingStrategyName, " +
		    "QualityScore, AveragePosition, FirstPageCpc, TopOfPageCpc,  " +
		    "CriteriaDestinationUrl, FinalUrls, Device  " +
		    "FROM   KEYWORDS_PERFORMANCE_REPORT " +
		    "WHERE  Clicks > 0 AND AdGroupStatus = 'ENABLED' " +
	  	    	    "DURING " + queryDateString + "," + queryDateString2;
	  		break;
	  	case "adg":
	  		query = "SELECT Date, AdGroupId, ExternalCustomerId, CampaignId, " +
		    "Impressions, Clicks, Ctr, ConvertedClicks, ConversionsManyPerClick, ClickConversionRate, ConversionValue, " +
		    "Cost, AverageCpc, CpcBid, CostPerConvertedClick, " +
		    "Labels, BiddingStrategyName, " +
		    "AveragePosition, Device " +
		    "FROM   ADGROUP_PERFORMANCE_REPORT " +
		    "WHERE  Clicks > 0 AND AdGroupStatus = 'ENABLED' " +
	  	    	    "DURING " + queryDateString + "," + queryDateString2;
	  		break;
	  	case "ad":
	  		query = "SELECT Date, Id, ExternalCustomerId, CampaignId, AdGroupId, KeywordId, " +
		    "Impressions, Clicks, Ctr, ConvertedClicks, ConversionsManyPerClick, ClickConversionRate, ConversionValue, " +
		    "Cost, AverageCpc, CostPerConvertedClick, " +
		    "Labels, " +
		    "AveragePosition, Device " +
		    "FROM   AD_PERFORMANCE_REPORT " +
		    "WHERE  Clicks > 0 AND AdGroupStatus = 'ENABLED' " +
	  	    	    "DURING " + queryDateString + "," + queryDateString2;
	  		break;
	  	case "cam_names":
	  		query = "SELECT Date, ExternalCustomerId, CampaignId, CampaignName, CampaignStatus " +
		    "FROM   CAMPAIGN_PERFORMANCE_REPORT " +
		    "WHERE  Clicks > 0 " +
    	    "DURING " + queryDateString + "," + queryDateString2;
	  		break;
	  	case "adg_names":
	  		query = "SELECT Date, AdGroupId, ExternalCustomerId, CampaignId, AdGroupName " +
		    "FROM   ADGROUP_PERFORMANCE_REPORT " +
		    "WHERE  Clicks > 0 AND AdGroupStatus = 'ENABLED' " +
    	    "DURING " + queryDateString + "," + queryDateString2;
	  		break;
	  	case "ad_names":
	  		query = "SELECT Date, Id, ExternalCustomerId, CampaignId, AdGroupId, " +
		    "Headline, Description1, Description2 " +
		    "FROM   AD_PERFORMANCE_REPORT " +
		    "WHERE  Clicks > 0 AND AdGroupStatus = 'ENABLED' " +
    	    "DURING " + queryDateString + "," + queryDateString2;
	  		break;
	  	default:
	  		System.out.println("returnQueryString(): " + queryType + " not recognised");
	  } 
	  
	  return query;
	  
  }
  
}
