package org.opendedup.util;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PatternLayout;

public class SDFSLogger {

	private static Logger log = Logger.getLogger("sdfs");
	static {
		ConsoleAppender app = new ConsoleAppender(new PatternLayout("%d [%t] %p %c %x - %m%n"));
		 BasicConfigurator.configure(app);
		 log.setLevel(Level.INFO);
	}
	
	public static Logger getLog() {
		return log;
	}
}
