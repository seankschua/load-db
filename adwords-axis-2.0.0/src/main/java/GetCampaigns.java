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

import com.google.api.ads.adwords.axis.factory.AdWordsServices;
import com.google.api.ads.adwords.axis.utils.v201502.SelectorBuilder;
import com.google.api.ads.adwords.axis.v201502.cm.Campaign;
import com.google.api.ads.adwords.axis.v201502.cm.CampaignPage;
import com.google.api.ads.adwords.axis.v201502.cm.CampaignServiceInterface;
import com.google.api.ads.adwords.axis.v201502.cm.Selector;
import com.google.api.ads.adwords.lib.client.AdWordsSession;
import com.google.api.ads.adwords.lib.selectorfields.v201502.cm.CampaignField;
import com.google.api.ads.common.lib.auth.OfflineCredentials;
import com.google.api.ads.common.lib.auth.OfflineCredentials.Api;
import com.google.api.client.auth.oauth2.Credential;

/**
 * This example gets all campaigns. To add a campaign, run AddCampaign.java.
 *
 * Credentials and properties in {@code fromFile()} are pulled from the
 * "ads.properties" file. See README for more info.
 *
 * Tags: CampaignService.get
 *
 * @author Adam Rogal
 */
public class GetCampaigns {

  private static final int PAGE_SIZE = 100;

  public static void main(String[] args) throws Exception {
    // Generate a refreshable OAuth2 credential similar to a ClientLogin token
    // and can be used in place of a service account.
    Credential oAuth2Credential = new OfflineCredentials.Builder()
        .forApi(Api.ADWORDS)
        .fromFile()
        .build()
        .generateCredential();

    // Construct an AdWordsSession.
    AdWordsSession session = new AdWordsSession.Builder()
        .fromFile()
        .withOAuth2Credential(oAuth2Credential)
        .build();

    AdWordsServices adWordsServices = new AdWordsServices();

    runExample(adWordsServices, session);
  }

  public static void runExample(
      AdWordsServices adWordsServices, AdWordsSession session) throws Exception {
    // Get the CampaignService.
    CampaignServiceInterface campaignService =
        adWordsServices.get(session, CampaignServiceInterface.class);

    int offset = 0;

    // Create selector.
    SelectorBuilder builder = new SelectorBuilder();
    Selector selector = builder
        .fields(CampaignField.Id, CampaignField.Name)
        .orderAscBy(CampaignField.Name)
        .offset(offset)
        .limit(PAGE_SIZE)
        .build();

    CampaignPage page = null;
    do {
      // Get all campaigns.
      page = campaignService.get(selector);

      // Display campaigns.
      if (page.getEntries() != null) {
        for (Campaign campaign : page.getEntries()) {
          System.out.println("Campaign with name \"" + campaign.getName() + "\" and id \""
              + campaign.getId() + "\" was found.");
        }
      } else {
        System.out.println("No campaigns were found.");
      }

      offset += PAGE_SIZE;
      selector = builder.increaseOffsetBy(PAGE_SIZE).build();
    } while (offset < page.getTotalNumEntries());
  }
}
