package org.opendedup.sdfs.io.events;

public class CloudSyncDLRequest {
	long volumeID;
	boolean updateHashMap;

	public CloudSyncDLRequest(long volumeID,boolean updateHashMap) {
		this.volumeID = volumeID;
		this.updateHashMap = updateHashMap;
	}
	
	public long getVolumeID() {
		return this.volumeID;
	}
	
	public boolean isUpdateHashMap() {
		return this.updateHashMap;
	}
	
}
