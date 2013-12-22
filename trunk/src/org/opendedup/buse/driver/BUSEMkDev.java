package org.opendedup.buse.driver;

import java.util.logging.Logger;

public class BUSEMkDev {

	private static Logger log = Logger.getLogger(BUSEMkDev.class.getName());

	static {
		System.loadLibrary("jbuse");
	}


	public static int startdev(final String dev, long sz, int blksz, BUSE buse,
			boolean readonly) throws Exception {
		
		
		log.info("Mounted filesystem");
		//ShutdownHook t = new ShutdownHook(dev,buse);
		//Runtime.getRuntime().addShutdownHook(t);
		int z = mkdev(dev, sz, blksz,buse, readonly);
		
		log.info("Filesystem is unmounted");
		return z;
	}

	public static void closeDev(final String dev) throws Exception {
		closedev(dev);
	}
	
	public static void init() {
		ThreadGroup threadGroup = new ThreadGroup(Thread.currentThread()
				.getThreadGroup(), "BUSE Threads");
		threadGroup.setDaemon(true);
		init(threadGroup);
	}

	private static native int mkdev(String dev, long sz,int blksz, BUSE buse,
			boolean readonly) throws Exception;
	

	private static native void closedev(String dev) throws Exception;
	
	private static native void init(ThreadGroup threadGroup);
	
	public static native void release();

}
