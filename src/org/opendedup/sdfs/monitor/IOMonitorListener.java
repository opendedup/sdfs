package org.opendedup.sdfs.monitor;

public interface IOMonitorListener {
	void actualBytesWrittenChanged(long total,int change,IOMonitor mon);
	void bytesReadChanged(long total,int change,IOMonitor mon);
	void duplicateBlockChanged(long total,IOMonitor mon);
	void rioChanged(long total,IOMonitor mon);
	void virtualBytesWrittenChanged(long total,int change,IOMonitor mon);
	void wioChanged(long total,IOMonitor mon);
	void clearAllCountersExecuted(long total,IOMonitor mon);
	void clearFileCountersExecuted(long total,IOMonitor mon);
	void removeDuplicateBlockChanged(long total,IOMonitor mon);
	void actualBytesWrittenChanged(long total,long change,IOMonitor mon);
	void bytesReadChanged(long total,long change,IOMonitor mon);
	void duplicateBlockChanged(long total,long change,IOMonitor mon);
	void virtualBytesWrittenChanged(long total,long change,IOMonitor mon); 
}
