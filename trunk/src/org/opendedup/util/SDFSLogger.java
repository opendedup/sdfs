package org.opendedup.util;

import java.io.IOException;


import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.opendedup.sdfs.Main;

public class SDFSLogger {

	private static Logger log = Logger.getLogger("sdfs");
	private static Logger basicLog = Logger.getLogger("bsdfs");
	static {
		ConsoleAppender bapp = new ConsoleAppender(new PatternLayout("%m%n"));
		basicLog.addAppender(bapp);
		basicLog.setLevel(Level.INFO);
		RollingFileAppender app = null;
		try {

			app = new RollingFileAppender(new PatternLayout(
					"%d [%t] %p %c %x - %m%n"), Main.logPath, true);
			app.setMaxBackupIndex(2);
			app.setMaxFileSize("10MB");
		} catch (IOException e) {
			log.debug("unable to change appender", e);
		}
		log.addAppender(app);
		log.setLevel(Level.INFO);
	}

	public static Logger getLog() {
		return log;
	}
	
	public static void infoConsoleMsg(String msg) {
		System.out.println(msg);
		log.info(msg);
	}
	
	public static Logger getBasicLog() {
		return basicLog;
	}

	public static void setLevel(int level) {
		if (level == 0)
			log.setLevel(Level.DEBUG);
		else
			log.setLevel(Level.INFO);
	}

	public static void setToFileAppender(String file) {
		try {
			log.removeAllAppenders();
		} catch (Exception e) {

		}
		RollingFileAppender app = null;
		try {
			app = new RollingFileAppender(new PatternLayout(
					"%d [%t] %p %c %x - %m%n"), file, true);
			app.setMaxBackupIndex(2);
			app.setMaxFileSize("10MB");
		} catch (IOException e) {
			System.out.println("Unable to initialize logger");
			e.printStackTrace();
		}
		log.addAppender(app);
		log.setLevel(Level.INFO);
	}
}
