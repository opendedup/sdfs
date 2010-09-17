package org.opendedup.util;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

public class SDFSLogger {

	private static Logger log = Logger.getLogger("sdfs");
	static {
		 BasicConfigurator.configure();
		 log.setLevel(Level.INFO);
	}
	
	public static Logger getLog() {
		return log;
	}
}
