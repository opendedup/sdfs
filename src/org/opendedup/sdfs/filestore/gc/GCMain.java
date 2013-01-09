package org.opendedup.sdfs.filestore.gc;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class GCMain {
	public static final ReentrantReadWriteLock gclock = new ReentrantReadWriteLock();
	/*
	public static final ReentrantLock gcRunningLock = new ReentrantLock();
	private static boolean gcRunning = false;
	
	public static boolean isLocked() {
		try {
			gcRunningLock.lock();
			return gcRunning;
		} finally {
			gcRunningLock.unlock();
		}
	}
	
	public static void lock() throws IOException {
		try {
			gcRunningLock.lock();
			if(isLocked())
				throw new IOException("Already Locked");
			gcRunning = true;
		} finally {
			gcRunningLock.unlock();
		}
	}
	
	public static void unlock() {
		try {
			gcRunningLock.lock();
			gcRunning = false;
		} finally {
			gcRunningLock.unlock();
		}
	}
	*/
	

}
