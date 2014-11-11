package main.java.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;

public class LocalUtils {

	public static String formatDate(Date date) {
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
        Date timestamp = new GregorianCalendar().getTime();
        String timestamp_formated = sdf.format(timestamp);
        return timestamp_formated;
	}
}
