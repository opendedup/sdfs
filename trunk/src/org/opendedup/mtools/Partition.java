package org.opendedup.mtools;

public class Partition {
	String device;
	boolean boot;
	long start;
	long end;
	long blocks;
	int System;
	String type;

	public String getDevice() {
		return device;
	}

	public void setDevice(String device) {
		this.device = device;
	}

	public boolean isBoot() {
		return boot;
	}

	public void setBoot(boolean boot) {
		this.boot = boot;
	}

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getEnd() {
		return end;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public long getBlocks() {
		return blocks;
	}

	public void setBlocks(long blocks) {
		this.blocks = blocks;
	}

	public int getSystem() {
		return System;
	}

	public void setSystem(int system) {
		System = system;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return this.device + " " + this.blocks + " " + this.start + " "
				+ this.end + " " + this.type + " " + this.System;
	}

}
