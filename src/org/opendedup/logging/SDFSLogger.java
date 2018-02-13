/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.logging;

import java.io.IOException;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.opendedup.sdfs.Main;

public class SDFSLogger {

	private static Logger log = Logger.getLogger("sdfs");
	private static Logger awslog = Logger.getLogger("com.amazonaws");
	private static Logger quartzLog = Logger.getLogger("org.quartz");
	private static Logger fslog = Logger.getLogger("fs");
	private static Logger basicLog = Logger.getLogger("bsdfs");
	private static boolean debug = false;
	private static boolean fsdebug = false;
	static RollingFileAppender app = null;
	static {

		ConsoleAppender bapp = new ConsoleAppender(new PatternLayout("%m%n"));
		basicLog.addAppender(bapp);

		basicLog.setLevel(Level.WARN);
		try {

			app = new RollingFileAppender(new PatternLayout(
					"%d [%p] [%c] [%C] [%L] [%t] %x - %m%n"), Main.logPath, true);
			app.setMaxBackupIndex(2);
			app.setMaxFileSize(Main.logSize);
		} catch (IOException e) {
			log.debug("unable to change appender", e);
		}
		awslog.setLevel(Level.WARN);
		quartzLog.setLevel(Level.INFO);
		awslog.removeAllAppenders();
		quartzLog.removeAllAppenders();
		log.setLevel(Level.INFO);
		fsdebug = true;
		fslog.setLevel(Level.INFO);
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.INFO);
		rootLogger.addAppender(app);
	}

	public static Logger getLog() {
		return log;

	}

	public static Logger getFSLog() {
		return fslog;
	}

	public static void infoConsoleMsg(String msg) {
		System.out.println(msg);
		log.info(msg);
	}

	public static Logger getBasicLog() {
		return basicLog;
	}

	public static boolean isDebug() {
		return debug;
	}

	public static boolean isFSDebug() {
		return fsdebug;
	}
	
	public static void setLogSize(String size) {
		app.setMaxFileSize(size);
	}

	public static void setLevel(int level) {
		if (level == 0) {
			log.setLevel(Level.DEBUG);
			debug = true;
		} else {
			log.setLevel(Level.INFO);
			debug = false;
		}
	}

	public static void setFSLevel(int level) {
		if (level == 0) {
			fslog.setLevel(Level.DEBUG);
			fsdebug = true;
		} else {
			fslog.setLevel(Level.INFO);
			fsdebug = false;
		}
	}

	public static void setToFileAppender(String file) {
		try {
			log.removeAllAppenders();
		} catch (Exception e) {

		}
		RollingFileAppender app = null;
		try {
			app = new RollingFileAppender(new PatternLayout(
					"%d [%c] [%t] %x - %m%n"), file, true);
			app.setMaxBackupIndex(2);
			app.setMaxFileSize("10MB");
		} catch (IOException e) {
			System.out.println("Unable to initialize logger");
			e.printStackTrace();
		}
		log.addAppender(app);
		fslog.addAppender(app);
		log.setLevel(Level.INFO);
		fslog.setLevel(Level.DEBUG);
	}
}
