package org.opendedup.util;

import java.io.IOException;


import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PatternLayout;
import org.opendedup.sdfs.Main;

public class SDFSLogger {

	private static Logger log = Logger.getLogger("sdfs");
	static {
		Appender app = null;
		try {
			if(Main.logToConsole)
				app = new ConsoleAppender(new PatternLayout("%d [%t] %p %c %x - %m%n"));
			else
				app = new FileAppender(new PatternLayout("%d [%t] %p %c %x - %m%n"),Main.logPath,true);
		} catch (IOException e) {
			System.out.println("Unable to initialize logger");
			e.printStackTrace();
		}
		 BasicConfigurator.configure(app);
		 log.setLevel(Level.INFO);
	}
	
	public static Logger getLog() {
		return log;
	}
}
