package fuse.logging;

import org.apache.commons.logging.Log;

import java.io.PrintStream;
import java.util.*;

/**
 * User: peter Date: Nov 4, 2005 Time: 8:03:57 PM
 */
public class FuseLog implements Log {
	public static PrintStream trace = System.err;
	public static PrintStream debug = System.err;
	public static PrintStream info = System.err;
	public static PrintStream warn = System.err;
	public static PrintStream error = System.err;
	public static PrintStream fatal = System.err;

	public static void setOut(PrintStream out) {
		trace = debug = info = warn = error = fatal = out;
	}

	public static final String LEVEL_PREFIX = "fuse.logging.level";

	private static final int LEVEL_TRACE = 0;
	private static final int LEVEL_DEBUG = 1;
	private static final int LEVEL_INFO = 2;
	private static final int LEVEL_WARN = 3;
	private static final int LEVEL_ERROR = 4;
	private static final int LEVEL_FATAL = 5;

	private static final String[] levelNames = new String[] { "TRACE", "DEBUG",
			"INFO", "WARN", "ERROR", "FATAL" };

	private static final String formatPattern = "%1$tH:%1$tM:%1$tS.%1$tL %2$8s %3$5s [%4$s]: %5$s%n";

	private static final Levels levels = new Levels();

	private String name;

	public FuseLog(String name) {
		this.name = name;
	}

	//
	// implementation of Log interface

	@Override
	public boolean isDebugEnabled() {
		return false;
		//return levels.isDebugEnabled(name);
	}

	@Override
	public boolean isErrorEnabled() {
		return levels.isErrorEnabled(name);
	}

	@Override
	public boolean isFatalEnabled() {
		return levels.isFatalEnabled(name);
	}

	@Override
	public boolean isInfoEnabled() {
		return levels.isInfoEnabled(name);
	}

	@Override
	public boolean isTraceEnabled() {
		return levels.isTraceEnabled(name);
	}

	@Override
	public boolean isWarnEnabled() {
		return levels.isWarnEnabled(name);
	}

	@Override
	public void trace(Object object) {
		_log(LEVEL_TRACE, trace, object, null);
	}

	@Override
	public void trace(Object object, Throwable throwable) {
		_log(LEVEL_TRACE, trace, object, throwable);
	}

	@Override
	public void debug(Object object) {
		_log(LEVEL_DEBUG, debug, object, null);
	}

	@Override
	public void debug(Object object, Throwable throwable) {
		_log(LEVEL_DEBUG, debug, object, throwable);
	}

	@Override
	public void info(Object object) {
		_log(LEVEL_INFO, info, object, null);
	}

	@Override
	public void info(Object object, Throwable throwable) {
		_log(LEVEL_INFO, info, object, throwable);
	}

	@Override
	public void warn(Object object) {
		_log(LEVEL_WARN, warn, object, null);
	}

	@Override
	public void warn(Object object, Throwable throwable) {
		_log(LEVEL_WARN, warn, object, throwable);
	}

	@Override
	public void error(Object object) {
		_log(LEVEL_ERROR, error, object, null);
	}

	@Override
	public void error(Object object, Throwable throwable) {
		_log(LEVEL_ERROR, error, object, throwable);
	}

	@Override
	public void fatal(Object object) {
		_log(LEVEL_FATAL, fatal, object, null);
	}

	@Override
	public void fatal(Object object, Throwable throwable) {
		_log(LEVEL_FATAL, fatal, object, throwable);
	}

	//
	// logging routine

	private void _log(int levelValue, PrintStream stream, Object object,
			Throwable throwable) {
		if (levelValue >= levels.getLevelValue(name)) {
			String msg;

			if (object instanceof Throwable && throwable == null)
				throwable = (Throwable) object;

			msg = object.toString();

			stream.printf(formatPattern, new Date(), Thread.currentThread()
					.getName(), levelNames[levelValue], name, msg);
			if (throwable != null)
				throwable.printStackTrace(stream);
		}
	}

	private static class Levels {
		private Map<String, Integer> name2levelMap = new HashMap<String, Integer>();

		Levels() {
			Properties props = System.getProperties();
			for (@SuppressWarnings("rawtypes")
			Enumeration e = props.propertyNames(); e.hasMoreElements();) {
				String propName = (String) e.nextElement();
				if (propName.startsWith(LEVEL_PREFIX)) {
					String levelKey = (propName.length() == LEVEL_PREFIX
							.length()) ? "" : propName.substring(LEVEL_PREFIX
							.length() + 1);
					String levelName = props.getProperty(propName);
					int levelValue = -1;
					for (int i = 0; i < levelNames.length; i++) {
						if (levelNames[i].equals(levelName)) {
							levelValue = i;
							break;
						}
					}

					if (levelValue < 0)
						throw new IllegalArgumentException(
								"Invalid logging level specified for System property: "
										+ propName + ": " + levelName);

					name2levelMap.put(levelKey, new Integer(levelValue));
				}
			}
		}

		boolean isDebugEnabled(String name) {
			return getLevelValue(name) <= LEVEL_DEBUG;
		}

		boolean isErrorEnabled(String name) {
			return getLevelValue(name) <= LEVEL_ERROR;
		}

		boolean isFatalEnabled(String name) {
			return getLevelValue(name) <= LEVEL_FATAL;
		}

		boolean isInfoEnabled(String name) {
			return getLevelValue(name) <= LEVEL_INFO;
		}

		boolean isTraceEnabled(String name) {
			return getLevelValue(name) <= LEVEL_TRACE;
		}

		boolean isWarnEnabled(String name) {
			return getLevelValue(name) <= LEVEL_WARN;
		}

		@SuppressWarnings("unused")
		public String toString(String name) {
			return levelNames[getLevelValue(name)];
		}

		private int getLevelValue(String name) {
			while (true) {
				Integer levelValue = name2levelMap.get(name);
				if (levelValue != null)
					return levelValue.intValue();

				if (name.length() == 0) {
					break;
				} else {
					int lastDot = name.lastIndexOf('.');
					if (lastDot >= 0)
						name = name.substring(0, lastDot);
					else
						name = "";
				}
			}

			// default ROOT level
			return LEVEL_INFO;
		}
	}
}
