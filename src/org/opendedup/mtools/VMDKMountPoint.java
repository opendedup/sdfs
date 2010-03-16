package org.opendedup.mtools;

public class VMDKMountPoint {
	String loopBack;
	String mountPoint;

	public VMDKMountPoint(String loopBack, String mountPoint) {
		this.loopBack = loopBack;
		this.mountPoint = mountPoint;
	}

	public String getLoopBack() {
		return loopBack;
	}

	public void setLoopBack(String loopBack) {
		this.loopBack = loopBack;
	}

	public String getMountPoint() {
		return mountPoint;
	}

	public void setMountPoint(String mountPoint) {
		this.mountPoint = mountPoint;
	}

}
