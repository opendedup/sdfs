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

import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.RolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.TriggeringPolicy;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.opendedup.sdfs.Main;


public class SDFSLogger {
	private static Logger log = LogManager.getLogger("sdfs");
	private static String msgPattern = "%d [%p] [%c] [%C] [%L] [%t] %x - %m%n";
	static {
		createSdfsLogger();
	}

	protected static void removeAllAppenders(org.apache.logging.log4j.core.Logger logger) {
		Map<String, Appender> appenders = logger.getAppenders();
		if (appenders != null) {
			for (Appender appender : appenders.values()) {
				logger.removeAppender(appender);
			}
		}
	}

	public static void useConsoleLogger() {
		LoggerContext context = LoggerContext.getContext(false);
		Configuration configuration = context.getConfiguration();
		LoggerConfig loggerConfig = configuration.getLoggerConfig("sdfs");
		removeAllAppenders((org.apache.logging.log4j.core.Logger) log);
		PatternLayout layout = PatternLayout.newBuilder().withPattern(msgPattern).build();
		Appender appender = ConsoleAppender.newBuilder().setLayout(layout).setName("consoleappender").build();
		loggerConfig.addAppender(appender, null, null);
		context.updateLoggers();

	}

	public static void setLevel(int level) {
		LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		Configuration config = ctx.getConfiguration();
		LoggerConfig loggerConfig = config.getLoggerConfig("sdfs");
		if (level == 0) {
			loggerConfig.setLevel(Level.DEBUG);
		}else {
			loggerConfig.setLevel(Level.INFO);
		}
		ctx.updateLoggers();
	}

	public static void setLogSize(String size) {
		Main.logSize = size;
		createSdfsLogger();
	}

	public static void createSdfsLogger() {
		LoggerContext context = (LoggerContext) LogManager.getContext();
		Configuration config = context.getConfiguration();

		PatternLayout layout = PatternLayout.newBuilder().withPattern(msgPattern).build();
		TriggeringPolicy tp = SizeBasedTriggeringPolicy.createPolicy(Main.logSize);
		RolloverStrategy st = DefaultRolloverStrategy.newBuilder()
				.withMax(Integer.toString(Main.logFiles))
				.withMin("1")
				.withCompressionLevelStr(String.valueOf("false"))
				.withStopCustomActionsOnError(true)
				.withConfig(config)
				.build();
		Appender appender = RollingFileAppender.newBuilder().setLayout(layout).setName("rollingfileappender")
				.withStrategy(st).withPolicy(tp).build();
		org.apache.logging.log4j.core.Logger clog = (org.apache.logging.log4j.core.Logger) log;
		removeAllAppenders(clog);
		clog.addAppender(appender);
		context.updateLoggers();
	}

	public static Logger getLog() {
		return log;

	}

	public static Logger getFSLog() {
		return log;
	}

	public static void infoConsoleMsg(String msg) {
		System.out.println(msg);
		log.info(msg);
	}

	public static Logger getBasicLog() {
		return log;
	}

}
