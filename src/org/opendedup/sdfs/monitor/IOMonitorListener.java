package org.opendedup.sdfs.monitor;

public interface IOMonitorListener {
	void actualBytesWrittenChanged(int bytes,IOMonitor mon);
	void bytesReadChanged(int bytes,IOMonitor mon);
	void duplicateBlockChanged(IOMonitor mon);
	void rioChanged(IOMonitor mon);
	void virtualBytesWrittenChanged(int bytes,IOMonitor mon);
	void wioChanged(IOMonitor mon);
	void clearAllCountersExecuted(IOMonitor mon);
	void clearFileCountersExecuted(IOMonitor mon);
	void removeDuplicateBlockChanged(IOMonitor mon);
	void actualBytesWrittenChanged(long bytes,IOMonitor mon);
	void bytesReadChanged(long bytes,IOMonitor mon);
	void duplicateBlockChanged(long bytes,IOMonitor mon);
	void virtualBytesWrittenChanged(long bytes,IOMonitor mon); 
}
