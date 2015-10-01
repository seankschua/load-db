//Initialise commands
//javac App.java
//java -cp .;jdbc.jar App

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.net.*;
import java.io.*;
import java.util.regex.*;
import java.util.*;
import com.opencsv.*;
import com.mysql.*;
import org.gjt.mm.mysql.*;

public class writeDB {
	
	public static final String directory = System.getProperty("user.home") + File.separatorChar + "reports" + File.separatorChar;
	//public static final String csvFile = "C:\\Users\\schuakianshun\\reports\\sq-2015-9.csv";
	
	//killing EMOJIS
	public static String removeBadChars(String s) {
	  if (s == null) return null;
	  StringBuilder sb = new StringBuilder();
	  for(int i = 0 ; i < s.length() ; i++){ 
	    if (Character.isHighSurrogate(s.charAt(i))) continue;
	    sb.append(s.charAt(i));
	  }
	  return sb.toString();
	}
	
    public static void main(String[] args) {

    	List<String> csvList = new ArrayList<String>();
    	File[] files = new File(directory).listFiles();
    	//If this pathname does not denote a directory, then listFiles() returns null. 

    	for (File file : files) {
    	    if (file.isFile()) {
    	    	csvList.add(file.getName());
    	        //System.out.println(file.getName());
    	        //System.out.println(Arrays.toString(file.getName().split("[-.]")));
    	    }
    	}
    	
    	System.out.println("Set job queue for " + csvList.size() + " items.");
    	
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        PreparedStatement pstmt = null;

        String url = "jdbc:mysql://127.0.0.1:3306/expedia";
        String user = "root";
        String password = "5yTewStV";
		int counter = 0;
		int totalCounter = 0;
		int SQLErrorcounter = 0;
		ArrayList<String> SQLErrorList = new ArrayList<String>();
		int SQLErrorLimit = 10;
		String values = "";
		String query = "";
		CSVReader reader = null;
        try {
        	
        	con = DriverManager.getConnection(url, user, password);            
			st = con.createStatement();
			
			fileloop:
			for(String csvName:csvList){
				
				String[] csvNameTokens = csvName.split("[-.]");
				String csvFile =  directory + csvName;
				String csvReportType = csvNameTokens[0];
				String csvYear = csvNameTokens[1];
				String csvMonth = csvNameTokens[2];
				String clientId = csvNameTokens[3];
				//System.out.println("clientId: " + clientId);
				System.out.println("Reading from " + csvName + "...");
				
				if (csvReportType.contentEquals("ad_names") || csvReportType.contentEquals("sq") || csvReportType.contentEquals("kw")){
					reader = new CSVReader(new InputStreamReader(new FileInputStream(csvFile), "UTF-16"),'\t');
				} else {
					reader = new CSVReader(new FileReader(csvFile));
				}
				String [] nextLine;
				//stupid google useless first line
				reader.readNext();
				String[] headerArray = reader.readNext();
				
				String deleteExisting = "";
				
				if(csvReportType.contentEquals("adgn")){
					deleteExisting = "DELETE FROM " + csvReportType + " WHERE Customer_ID=" + clientId + ";";
				} else {
					deleteExisting = "DELETE FROM " + csvReportType + " WHERE YEAR(Day)=" + csvYear
							+ " AND MONTH(Day)=" + csvMonth+ " AND Customer_ID=" + clientId + ";";
				}

				//System.out.println(deleteExisting);
				int deleteResult = st.executeUpdate(deleteExisting);
				if (deleteResult>0){
					System.out.println(deleteResult + ": deleted " + csvYear+ "-" + csvMonth+ " date rows in table " + csvReportType);
				}
				
				rs = st.executeQuery("SELECT * FROM " + csvReportType + " LIMIT 1;");
				ResultSetMetaData rsmd = rs.getMetaData();
				int numberOfColumns = rsmd.getColumnCount();
				String columnString = "";
				String ques = "";
				//skip the id column
				for(int i=2;i<=numberOfColumns;i++){
					columnString += ", " + rsmd.getColumnName(i) + "";
					ques += ", ?";
				}
				columnString = columnString.substring(2);
				ques = ques.substring(2);  
				
				query  = "INSERT INTO " + csvReportType + "("
						+ columnString
						+ ")"
						+" VALUES(" + ques + ")";
				
				//System.out.println(query);
				
				pstmt = con.prepareStatement(query);
				
				while ((nextLine = reader.readNext()) != null) {
					
					counter++;
					if (counter==1){
						System.out.print("Writing rows to table " + csvReportType + "...");
					}
					if (counter%10000==0){
						System.out.print(counter + "...");
					}
					if (counter%100000==0){
						System.out.println("");
					}
					
					if(nextLine[0].equalsIgnoreCase("Total")){
						break;
					}
					
					nextLine = cleanLine(nextLine,csvReportType);
					//System.out.println(Arrays.toString(nextLine));
					//System.out.println(counter + ": " + nextLine[2]);

					//need some length error detection here
					for(int i=2;i<=numberOfColumns;i++){
						String columnType = rsmd.getColumnTypeName(i);
						//System.out.println(columnType);
						//System.out.println(rsmd.getColumnName(i));
						//System.out.println(nextLine[i-2]);
						pstmt.setObject(i-1, nextLine[i-2]);
					}
					
					try{
						pstmt.executeUpdate();
					}catch (SQLException e){
						System.out.println("");
						SQLErrorcounter++;
						System.out.println(e.getMessage() + ", count: " + SQLErrorcounter);
						System.out.println(Arrays.toString(nextLine));
						SQLErrorList.add(csvName + ": " + Arrays.toString(nextLine));
						if (SQLErrorcounter>SQLErrorLimit){
							System.out.println(SQLErrorLimit + " errors breached. Stopping Execution.");
							for (String errorLine : SQLErrorList){
								System.out.println(errorLine);
							}
							
						}
						break fileloop;
					}
					
					//System.out.println("pstmt.executeUpdate() " + pstmt.executeUpdate());
					//break;
				}
				System.out.println("");
				System.out.println("From " + csvName + " wrote a total " + counter + " rows to table " + csvReportType + ".");
				totalCounter = totalCounter + counter;
				counter = 0;
			}
			
        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(writeDB.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
		
        } catch(Exception e){
			e.printStackTrace();
		} finally {
            try {
                if (st != null) {
                    st.close();
                }
                if (con != null) {
                    con.close();
                }
                if (pstmt != null) {
                	pstmt.close();
                }
                if (reader != null) {
                	reader.close();
                }

            } catch (SQLException ex) {
                Logger lgr = Logger.getLogger(writeDB.class.getName());
                lgr.log(Level.WARNING, ex.getMessage(), ex);
            } catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
        }
        System.out.println("writeDB() completed with " + totalCounter + " rows, " + csvList.size() + " files, " + SQLErrorcounter + " SQL errors.");
        
    }
    
    public static String[] cleanLine(String[] nextLine, String tableName) {
    	
    	switch(tableName){
    		case "sq":
    			
    			if (nextLine[7].equalsIgnoreCase(" --")){
					nextLine[7] = null;
				}
    			
				if (nextLine[20].equalsIgnoreCase("")){
					nextLine[20] = null;
				}
				if (nextLine[21].equalsIgnoreCase("")){
					nextLine[21] = null;
				}
				if (nextLine[22].equalsIgnoreCase("")){
					nextLine[22] = null;
				}
				
    			nextLine[12] = nextLine[12].replace("%", "");
				//System.out.println(nextLine[11]);
				nextLine[15] = nextLine[15].replace("%", "");
				nextLine[16] = nextLine[16].replace(",", "");
				nextLine[17] = Float.toString(Float.parseFloat(nextLine[17])/1000000);
				nextLine[18] = Float.toString(Float.parseFloat(nextLine[18])/1000000);
				nextLine[19] = Float.toString(Float.parseFloat(nextLine[19])/1000000);
				
				if(!nextLine[22].contentEquals("Computers")){
					nextLine[2] = removeBadChars(nextLine[2]);
				}
				
				break;
    		
    		case "kw":
    			
    			if (nextLine[16].equalsIgnoreCase(" --")){
					nextLine[16] = null;
				} else {
					nextLine[16] = Float.toString(Float.parseFloat(nextLine[16])/1000000);
				}
    			if (nextLine[18].equalsIgnoreCase(" --")){
					nextLine[18] = null;
				}
    			if (nextLine[19].equalsIgnoreCase(" --")){
					nextLine[19] = null;
				}
    			
    			if (nextLine[24].equalsIgnoreCase("")){
					nextLine[24] = null;
				}
    			if (nextLine[25].equalsIgnoreCase("")){
					nextLine[25] = null;
				}
    			if (nextLine[26].equalsIgnoreCase("")){
					nextLine[26] = null;
				}
    			
    			nextLine[9] = nextLine[9].replace("%", "");
				//System.out.println(nextLine[11]);
				nextLine[12] = nextLine[12].replace("%", "");
				nextLine[13] = nextLine[13].replace(",", "");
				nextLine[14] = Float.toString(Float.parseFloat(nextLine[14])/1000000);
				nextLine[15] = Float.toString(Float.parseFloat(nextLine[15])/1000000);
				nextLine[17] = Float.toString(Float.parseFloat(nextLine[17])/1000000);
				nextLine[22] = Float.toString(Float.parseFloat(nextLine[22])/1000000);
				nextLine[23] = Float.toString(Float.parseFloat(nextLine[23])/1000000);
								
				break;
    		case "adg":
    			
    			if (nextLine[13].equalsIgnoreCase(" --")){
					nextLine[13] = null;
				} else {
					nextLine[13] = Float.toString(Float.parseFloat(nextLine[13])/1000000);
				}
    			if (nextLine[15].equalsIgnoreCase(" --")){
					nextLine[15] = null;
				}
    			if (nextLine[16].equalsIgnoreCase(" --")){
					nextLine[16] = null;
				}
    			if (nextLine[18].equalsIgnoreCase(" --")){
					nextLine[18] = null;
				}
    			
    			nextLine[6] = nextLine[6].replace("%", "");
    			nextLine[9] = nextLine[9].replace("%", "");
    			nextLine[10] = nextLine[10].replace(",", "");
    			nextLine[11] = Float.toString(Float.parseFloat(nextLine[11])/1000000);
    			nextLine[12] = Float.toString(Float.parseFloat(nextLine[12])/1000000);
								
				break;
    		case "ad":
    			
    			if (nextLine[16].equalsIgnoreCase(" --")){
					nextLine[16] = null;
				}
    			if (nextLine[18].equalsIgnoreCase(" --")){
					nextLine[18] = null;
				}
    			
    			nextLine[8] = nextLine[8].replace("%", "");
    			nextLine[11] = nextLine[11].replace("%", "");
    			nextLine[12] = nextLine[12].replace(",", "");
    			nextLine[13] = Float.toString(Float.parseFloat(nextLine[13])/1000000);
    			nextLine[14] = Float.toString(Float.parseFloat(nextLine[14])/1000000);
    			
    			break;
    		case "cam_names":
    			break;
    		case "adg_names":
    			break;
    		case "ad_names":
    			
    			if (nextLine[5].equalsIgnoreCase("")){
					nextLine[5] = null;
				}
    			
    			break; 
    		case "adgn":
    			//doesn't improve query performance at all
    			
    			//System.out.println(nextLine.length);
    			nextLine = Arrays.copyOf(nextLine, nextLine.length + 1);
    			//System.out.println(nextLine.length);
    			//System.out.println(Arrays.toString(nextLine));
    			nextLine[nextLine.length-1] = Integer.toString(nextLine[3].contains("DSA")?1:0);
    			
    			break;
			default:
				System.out.println("cleanLine(): " + tableName + " not recognised");
    	}
    	
    	return nextLine;
    }
}