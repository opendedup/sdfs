package org.opendedup.sdfs.notification;

public class FDiskEvent extends SDFSEvent {
	

	/**
	 * 
	 */
	private static final long serialVersionUID = -4794152778857423029L;
	
	public FDiskEvent(String shortMsg) {
		super(FDISK, getTarget(), shortMsg, RUNNING);
	}

}
