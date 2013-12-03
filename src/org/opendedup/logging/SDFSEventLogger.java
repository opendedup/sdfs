package org.opendedup.logging;

import java.io.IOException;
import java.util.Date;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.slf4j.MDC;

public class SDFSEventLogger {

	private static Logger log = null;
	static {
		String evtLogPath = Main.logPath.substring(0,
				Main.logPath.lastIndexOf("."))
				+ "-events.log";
		RollingFileAppender app = null;

		try {

			app = new RollingFileAppender(
					new PatternLayout(
							"%X{level},%X{type},%X{target},%X{shortMsg},%X{longMsg},%X{startTime},%X{endTime},%X{uid},%X{extendedInfo},%X{maxCt},%X{curCt}\n"),
					evtLogPath, true);
			app.setMaxBackupIndex(2);
			app.setMaxFileSize("10MB");
			app.activateOptions();
		} catch (IOException e) {
			e.printStackTrace();
		}
		log = Logger.getLogger("eventlog");
		log.addAppender(app);
		log.setLevel(Level.INFO);
	}

	public static void init() {
		String evtLogPath = Main.logPath.substring(0,
				Main.logPath.lastIndexOf("."))
				+ "-events.log";
		System.out.println(evtLogPath);
		log = Logger.getLogger("eventlog");
		RollingFileAppender app = null;
		try {

			app = new RollingFileAppender(
					new PatternLayout(
							"%X{level},%X{type},%X{target},%X{shortMsg},%X{longMsg},%X{startTime},%X{endTime},%X{uid},%X{extendedInfo},%X{maxCt},%X{curCt}"),
					evtLogPath, true);
			app.setMaxBackupIndex(2);
			app.setMaxFileSize("10MB");
			app.activateOptions();
		} catch (IOException e) {
			log.debug("unable to change appender", e);
		}
		log.addAppender(app);
		log.setLevel(Level.INFO);
	}

	public static void setLevel(int level) {
		if (level == 0)
			log.setLevel(Level.DEBUG);
		else
			log.setLevel(Level.INFO);
	}

	public static synchronized void log(SDFSEvent evt) {
		MDC.put("level", evt.level.toString());
		MDC.put("uid", evt.uid);
		MDC.put("type", evt.type.toString());
		MDC.put("target", evt.target);
		MDC.put("shortMsg", evt.shortMsg);
		MDC.put("longMsg", evt.longMsg);
		MDC.put("startTime", new Date(evt.startTime).toString());
		MDC.put("endTime", new Date(evt.endTime).toString());
		MDC.put("extendedInfo", evt.extendedInfo);
		MDC.put("maxCt", Long.toString(evt.maxCt));
		MDC.put("curCt", Long.toString(evt.curCt));
		log.info("");
		MDC.clear();
	}

	public static void main(String[] args) {
		Main.logPath = "/tmp/volume-log-xml.log";
		SDFSEvent evt = SDFSEvent.testEvent("Archiving out");
		evt.maxCt = 10;
		evt.curCt = 1;
		evt.endEvent("woweessss");
	}

}
