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

import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;


public class SDFSLogger {

	private static Logger log = LogManager.getLogger("sdfs");
	private static Logger fslog = LogManager.getLogger("fs");
	private static Logger basicLog = LogManager.getLogger("bsdfs");
	static RollingFileAppender app = null;


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

	public static void useConsoleLogger() {

	}



}
