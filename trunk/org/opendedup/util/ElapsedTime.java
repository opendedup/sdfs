package org.opendedup.util;

import java.util.Date;
import java.text.SimpleDateFormat;

public class ElapsedTime {

	public static String getDateTime(Date date) {
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SS");
		return sdf.format(date);
	}

}
