package org.opendedup.buse.driver;

import java.util.logging.Level;

import java.util.logging.Logger;

public class BUSEMkDev {

	private static Logger log = Logger.getLogger(BUSEMkDev.class.getName());

	static {
		System.loadLibrary("jbuse");
	}

	public static int mkdev(final String dev, long sz, int blksz, BUSE buse,
			boolean readonly) throws Exception {
		ThreadGroup threadGroup = new ThreadGroup(Thread.currentThread()
				.getThreadGroup(), "FUSE Threads");
		threadGroup.setDaemon(true);

		log.info("Mounted filesystem");
		ShutdownHook t = new ShutdownHook(dev,buse);
		Runtime.getRuntime().addShutdownHook(t);
		int z = mkdev(dev, sz, blksz,buse, readonly, threadGroup);
		
		log.info("Filesystem is unmounted");

		if (log.isLoggable(Level.FINEST)) {
			int n = threadGroup.activeCount();
			log.finest("ThreadGroup(\"" + threadGroup.getName()
					+ "\").activeCount() = " + n);

			Thread[] threads = new Thread[n];
			threadGroup.enumerate(threads);
			for (int i = 0; i < threads.length; i++) {
				log.finest("thread[" + i + "] = " + threads[i]
						+ ", isDaemon = " + threads[i].isDaemon());
			}
		}
		return z;
	}

	public static void closeDev(final String dev) throws Exception {
		closedev(dev);
	}

	private static native int mkdev(String dev, long sz,int blksz, BUSE buse,
			boolean readonly, ThreadGroup threadGroup) throws Exception;

	private static native void closedev(String dev) throws Exception;

}
