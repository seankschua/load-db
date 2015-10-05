import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.TimeZone;

import com.opencsv.CSVWriter;

public class repeatQuery {
	
	public static final String directory = System.getProperty("user.home") + File.separatorChar + "query" + File.separatorChar;
	
	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        PreparedStatement pstmt = null;

        String url = Utility.url;
        String user = Utility.user;
        String password = Utility.password;
        
        int counter= 0;
        int totalCounter=0;
        TimeZone timeZone = TimeZone.getTimeZone("Singapore");
        Calendar calendar = Calendar.getInstance(timeZone);
        SimpleDateFormat sdf = 
               new SimpleDateFormat("HH:mm:ss");
        sdf.setTimeZone(timeZone);

        ArrayList<String> queryList = new ArrayList<String>(Arrays.asList("matchtype","matchtypeDSA"));
		//ArrayList<String> clientList = new ArrayList<String>(Arrays.asList("1074194775"));
		ArrayList<String> clientList = Utility.accounts;
		ArrayList<String> yearList = new ArrayList<String>(Arrays.asList("2015"));
		ArrayList<String> monthList = new ArrayList<String>(Arrays.asList("1","2","3","4","5","6"));
		//ArrayList<String> monthList = new ArrayList<String>(Arrays.asList("8","9"));
		//ArrayList<String> monthList = Utility.monthList;
        
        try {
			con = DriverManager.getConnection(url, user, password);
			st = con.createStatement();
			CSVWriter wr = null;
			Calendar start = Calendar.getInstance(timeZone);
			
			for (String type:queryList){
				System.out.print("Executing query type " + type + "...");
				wr = new CSVWriter(new FileWriter(directory + type + ".csv"));
				start = Calendar.getInstance(timeZone);
		        System.out.println("Query started at " + sdf.format(start.getTime()));
				for (String client:clientList){
					System.out.println("Executing query for client: " + client + "...");
					for (String year:yearList){
						System.out.println("Executing query for Year: " + year + "...");
						for (String month:monthList){
							System.out.println("Executing query for Month: " + month + "...");
							
							rs = st.executeQuery(query(type,client,year,month));
							wr.writeAll(rs, counter==0);
							counter++;
							totalCounter++;
							//Thread.sleep(2*1000);
							calendar = Calendar.getInstance(timeZone);
							System.out.println("Written resultset " + counter + " at " + sdf.format(calendar.getTime()));
						}
					}
				}
				wr.flush();
				wr.close();
				calendar = Calendar.getInstance(timeZone);
				System.out.print("Query started at " + sdf.format(start.getTime()) + ", ");
				System.out.print("ended at " + sdf.format(calendar.getTime()) + ", ");
				System.out.print("total resultsets written: " + counter);
				System.out.println();
				System.out.println();
				counter = 0;
			}
			System.out.println("repeatQuery() completed with " + queryList.size() + " files, " + totalCounter + " resultsets.");
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}            
		
	}
	
	public static String query(String type, String client, String year, String month){
		
		String toReturn = "";
		
		switch(type){
			case "test":
				toReturn = "select * from sq limit 10";
				break;
			case "matchtypeDSA":
				toReturn =	"select cid.ClientName, sq.day, dsa, Match_type_variant, count(*) as count, " 
						+	"sum(impressions) as impressions, sum(clicks) as clicks, sum(Converted_clicks) as Converted_clicks, sum(conversions) as conversions, " 
						+	"sum(Total_conv_value) as GP, sum(cost) as cost, "
						+	"sum(clicks)/sum(impressions) as CTR, sum(Converted_clicks)/sum(clicks) as CVR, sum(conversions)/sum(clicks) as CVR2, "
						+	"sum(Total_conv_value)/sum(clicks) as GPpC, sum(cost)/sum(clicks) as CpC " 
						+	"from sq " 
						+	"inner join cid " 
						+	"on sq.Customer_ID = cid.ClientId " 
						+	"left join adgn " 
						+	"on sq.ad_group_id = adgn.Ad_group_ID " 
						+	"where month(sq.day)=" + month + " "
						+	"and sq.Customer_ID=" + client + " " 
						+	"group by cid.ClientName, day, Match_type_variant, DSA";
				break;
			case "matchtype":
				toReturn =	"select cid.ClientName, sq.day, 'Total' as dsa, Match_type_variant, count(*) as count, "
						+	"sum(impressions) as impressions, sum(clicks) as clicks, sum(Converted_clicks) as Converted_clicks, sum(conversions) as conversions, " 
						+	"sum(Total_conv_value) as GP, sum(cost) as cost, "
						+	"sum(clicks)/sum(impressions) as CTR, sum(Converted_clicks)/sum(clicks) as CVR, sum(conversions)/sum(clicks) as CVR2, "
						+	"sum(Total_conv_value)/sum(clicks) as GPpC, sum(cost)/sum(clicks) as CpC " 
						+	"from sq " 
						+	"inner join cid " 
						+	"on sq.Customer_ID = cid.ClientId "
						+	"where month(sq.day)=" + month + " "
						+	"and sq.Customer_ID=" + client + " " 
						+	"group by cid.ClientName, day, Match_type_variant";
				break;
			default:
				break;
		}
		return toReturn;			
	}

}
