package org.opendedup.sdfs.filestore;

import java.io.IOException;
import java.util.logging.Level;

import java.util.logging.Logger;

import org.opendedup.sdfs.io.DedupFile;

/**
 * 
 * @author Sam Silverberg
 * 
 *         This class initiates a thread that is used to monitor open files
 *         within the DedupFileStore. It will close them if left open and
 *         untouched for longer than the @see
 *         com.annesam.sdfs.Main#maxInactiveFileTime .
 * 
 */
public class OpenFileMonitor implements Runnable {

	int interval = 60000;
	int maxInactive = 900000;
	boolean closed = false;
	Thread th = null;
	private static Logger log = Logger.getLogger("sdfs");

	/**
	 * Instantiates the OpenFileMonitor
	 * 
	 * @param interval
	 *            the interval to check for inactive files
	 * @param maxInactive
	 *            the maximum time allowed for a file to be inactive. Inactivity
	 *            is determined by last accessed time.
	 */
	public OpenFileMonitor(int interval, int maxInactive) {
		this.interval = interval;
		this.maxInactive = maxInactive;
		th = new Thread(this);
		th.start();
	}

	public void run() {
		while (!closed) {
			try {
				Thread.sleep(this.interval);
			} catch (InterruptedException e) {
				if (this.closed)
					break;
			}
			try {
				DedupFile[] files = DedupFileStore.getArray();
				for (int i = 0; i < files.length; i++) {
					DedupFile df = files[i];
					if (this.isFileStale(df)) {
						try {
							df.close();
						} catch (Exception e) {
							log.log(Level.WARNING, "Unable close file for "
									+ df.getMetaFile().getPath(), e);
						}
					}
				}
			} catch (Exception e) {
				log.log(Level.WARNING, "Unable check files", e);
			}
		}
	}

	/**
	 * Checks if a file should be closed
	 * 
	 * @param df
	 *            the DedupFile to check
	 * @return true if stale.
	 * @throws IOException 
	 */
	public boolean isFileStale(DedupFile df) throws IOException {
		long currentTime = System.currentTimeMillis();
		long staleTime = MetaFileStore.getMF(df.getMetaFile().getPath())
				.getLastAccessed()
				+ this.maxInactive;
		return currentTime > staleTime;
	}

	/**
	 * Closes the OpenFileMonitor thread.
	 */
	public void close() {
		this.closed = true;
		th.interrupt();
	}

}
