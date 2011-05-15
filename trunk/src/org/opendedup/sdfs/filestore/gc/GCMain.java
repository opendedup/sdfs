package org.opendedup.sdfs.filestore.gc;

import java.util.concurrent.locks.ReentrantLock;

public class GCMain {
	public static final ReentrantLock gclock = new ReentrantLock();
	public static boolean gcRunning = false;

}
