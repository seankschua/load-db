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
	
	public static final String directory = "C:\\Users\\schuakianshun\\reports";
	//public static final String csvFile = "C:\\Users\\schuakianshun\\reports\\sq-2015-9.csv";
	
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
    	
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        PreparedStatement pstmt = null;

        String url = "jdbc:mysql://127.0.0.1:3306/expedia";
        String user = "root";
        String password = "5yTewStV";
		int counter = 0;
		String values = "";
		String query = "";
        try {
        	
        	con = DriverManager.getConnection(url, user, password);            
			st = con.createStatement();
			
			for(String csvName:csvList){
				
				String[] csvNameTokens = csvName.split("[-.]");
				String csvFile =  directory + "\\" + csvName;
				String clientId = csvNameTokens[3];
				//System.out.println("clientId: " + clientId);
				System.out.println("Reading from " + csvName + "...");
				
				CSVReader reader = new CSVReader(new FileReader(csvFile));
				String [] nextLine;
				//stupid google useless first line
				reader.readNext();
				String[] headerArray = reader.readNext();
				
				String deleteExisting = "DELETE FROM " + csvNameTokens[0] + " WHERE YEAR(Day)=" + csvNameTokens[1] 
						+ " AND MONTH(Day)=" + csvNameTokens[2] + " AND Customer_ID=" + clientId + ";";
				//System.out.println(deleteExisting);
				int deleteResult = st.executeUpdate(deleteExisting);
				if (deleteResult>0){
					System.out.println(deleteResult + ": deleted " + csvNameTokens[1] + "-" + csvNameTokens[2] + " date rows in table " + csvNameTokens[0]);
				}
				
				rs = st.executeQuery("SELECT * FROM " + csvNameTokens[0] + " LIMIT 1;");
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
				
				query  = "INSERT INTO " + csvNameTokens[0] + "("
						+ columnString
						+ ")"
						+" VALUES(" + ques + ")";
				
				//System.out.println(query);
				
				pstmt = con.prepareStatement(query);
				
				while ((nextLine = reader.readNext()) != null) {
					
					counter++;
					if (counter%10000==0){
						System.out.println("Writing row " + counter + " to table " + csvNameTokens[0] + "...");
					}
					
					if(nextLine[0].equalsIgnoreCase("Total")){
						break;
					}
					
					nextLine = cleanLine(nextLine,csvNameTokens[0]);
					//System.out.println(Arrays.toString(nextLine));
					//System.out.println(nextLine[16]);

					//need some length error detection here
					for(int i=2;i<=numberOfColumns;i++){
						String columnType = rsmd.getColumnTypeName(i);
						//System.out.println(columnType);
						//System.out.println(rsmd.getColumnName(i));
						//System.out.println(nextLine[i-2]);
						pstmt.setObject(i-1, nextLine[i-2]);
					}
					
					pstmt.executeUpdate();
					//System.out.println("pstmt.executeUpdate() " + pstmt.executeUpdate());
					//break;
				}
				System.out.println("From " + csvName + " wrote a total " + counter + " rows to table " + csvNameTokens[0] + ".");
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

            } catch (SQLException ex) {
                Logger lgr = Logger.getLogger(writeDB.class.getName());
                lgr.log(Level.WARNING, ex.getMessage(), ex);
            }
        }
        
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
				
    			nextLine[12] = nextLine[12].replace("%", "");
				//System.out.println(nextLine[11]);
				nextLine[15] = nextLine[15].replace("%", "");
				nextLine[16] = nextLine[16].replace(",", "");
				nextLine[17] = Float.toString(Float.parseFloat(nextLine[17])/1000000);
				nextLine[18] = Float.toString(Float.parseFloat(nextLine[18])/1000000);
				nextLine[19] = Float.toString(Float.parseFloat(nextLine[19])/1000000);

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
    			
    			nextLine[9] = nextLine[9].replace("%", "");
				//System.out.println(nextLine[11]);
				nextLine[12] = nextLine[12].replace("%", "");
				nextLine[13] = nextLine[13].replace(",", "");
				nextLine[14] = Float.toString(Float.parseFloat(nextLine[14])/1000000);
				nextLine[15] = Float.toString(Float.parseFloat(nextLine[15])/1000000);
				nextLine[17] = Float.toString(Float.parseFloat(nextLine[17])/1000000);
								
				break;
			default:
				System.out.println("cleanLine(): " + tableName + " not recognised");
    	}
    	
    	return nextLine;
    }
}