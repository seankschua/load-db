import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

public class Utility {
	
	public static final String url = "jdbc:mysql://127.0.0.1:3306/expedia";
	public static final String user = "root";
	public static final String password = "5yTewStV";
	
	public static final ArrayList<String> accounts = new ArrayList<String>(
		    Arrays.asList("1074194775",
                    "1320665226",
                    "1862351155",
                    "2050852785",
                    "3041754936",
                    "4947289561",
                    "5366197879",
                    "6147151681",
                    "6258982657",
                    "6506475316",
                    "6526427655",
                    "7503648530",
                    "7557877834",
                    "7859896624",
                    "8784097667"));
	
	public static final ArrayList<String> monthList = new ArrayList<String>(Arrays.asList("1","2","3","4","5","6","7","8","9"));
	
	public static String timeElapsed(Date date1, Date date2){
		long difference = (date2.getTime() - date1.getTime())/1000;
		int seconds = (int)(difference%60);
		int minutes = (int)((difference/60)%60);
		int hours = (int)((difference/3600));
		return hours + "h " + minutes + "m " + seconds + "s";
	}
	
	public static String jobStatus(Date start, int jobCount, int currentJob, String currentJobName){
		Date currentDate = new Date();
		return "Job " + currentJob + "/" + jobCount + " " + currentJobName + " completed at " + currentDate + ", time elapsed: " + timeElapsed(start, currentDate);
	}
}
