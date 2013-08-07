package org.opendedup.sdfs.filestore.gc;



import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class GCMain {
	public static final ReentrantReadWriteLock gclock = new ReentrantReadWriteLock();
	public static final ReentrantLock gcRunningLock = new ReentrantLock();
	
	
	

}
