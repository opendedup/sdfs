package org.opendedup.sdfs.io;

public interface VolumeListenerInterface {
	void actualWriteBytesChanged(long change, double current, Volume vol);

	void duplicateBytesChanged(long change, double current, Volume vol);

	void readBytesChanged(long change, double current, Volume vol);

	void rIOChanged(long change, double current, Volume vol);

	void wIOChanged(long change, double current, Volume vol);

	void virtualBytesWrittenChanged(long change, double current, Volume vol);

	void allowExternalSymLinksChanged(boolean symlink, Volume vol);

	void capacityChanged(long capacity, Volume vol);

	void currentSizeChanged(long capacity, Volume vol);

	void usePerMonChanged(boolean perf, Volume vol);

	void started(Volume vol);

	void mounted(Volume vol);

	void unmounted(Volume vol);

	void stopped(Volume vol);
}
