package CreateOsmDatabases;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Log {
	
	public static void info(String message) {
		
		log("INFO: "+message);
	}
	
	public static void warning(String message) {
		
		log("((( WARNING ))):  "+message);
	}
	
	public static void error(String message) {
		
		log("((( ERROR ))):  "+message);
	}
	
	public static void debug(String message) {
		
		log("DEBUG: "+message);
	}
	
	private static void log(String message) {
		
		String timeStamp = new SimpleDateFormat("HH:mm:ss.SSS ").format(Calendar.getInstance().getTime());
		
		System.out.println(timeStamp+message);
	}

}
