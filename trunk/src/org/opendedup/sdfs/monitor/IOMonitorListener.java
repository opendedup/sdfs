package org.opendedup.sdfs.monitor;

public interface IOMonitorListener {
	void actualBytesWrittenChanged(long total, int change, IOMonitor mon);

	void bytesReadChanged(long total, int change, IOMonitor mon);

	void duplicateBlockChanged(long total, IOMonitor mon);

	void rioChanged(long total, IOMonitor mon);

	void virtualBytesWrittenChanged(long total, int change, IOMonitor mon);

	void wioChanged(long total, IOMonitor mon);

	void clearAllCountersExecuted(long total, IOMonitor mon);

	void clearFileCountersExecuted(long total, IOMonitor mon);

	void removeDuplicateBlockChanged(long total, IOMonitor mon);

	void actualBytesWrittenChanged(long total, long change, IOMonitor mon);

	void bytesReadChanged(long total, long change, IOMonitor mon);

	void duplicateBlockChanged(long total, long change, IOMonitor mon);

	void virtualBytesWrittenChanged(long total, long change, IOMonitor mon);

	void riopsChanged(int iops, int changed, IOMonitor mon);

	void wiopsChanged(int iops, int changed, IOMonitor mon);

	void iopsChanged(int iops, int changed, IOMonitor mon);

	void rmbpsChanged(long mbps, int changed, IOMonitor mon);

	void wmbpsChanged(long mbps, int changed, IOMonitor mon);

	void mbpsChanged(long mbps, int changed, IOMonitor mon);

	void qosChanged(int old, int newQos, IOMonitor mon);

	void ioProfileChanged(String old, String newProf, IOMonitor mon);
}
