package org.opendedup.sdfs.io.events;

public class CloudSyncDLRequest {
	long volumeID;

	public CloudSyncDLRequest(long volumeID) {
		this.volumeID = volumeID;
	}
	
	public long getVolumeID() {
		return this.volumeID;
	}
	
}
