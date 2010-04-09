package org.opendedup.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ElapsedTime {

	public static String getDateTime(Date date) {
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SS");
		return sdf.format(date);
	}

}
