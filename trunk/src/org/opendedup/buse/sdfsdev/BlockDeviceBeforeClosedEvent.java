package org.opendedup.buse.sdfsdev;

import org.opendedup.sdfs.io.BlockDev;

public class BlockDeviceBeforeClosedEvent {
	BlockDev dev;
	public BlockDeviceBeforeClosedEvent(BlockDev dev) {
		this.dev = dev;
	}

}
