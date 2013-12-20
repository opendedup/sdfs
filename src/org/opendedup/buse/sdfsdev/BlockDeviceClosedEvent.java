package org.opendedup.buse.sdfsdev;

import org.opendedup.sdfs.io.BlockDev;

public class BlockDeviceClosedEvent {
	BlockDev dev;
	public BlockDeviceClosedEvent(BlockDev dev) {
		this.dev = dev;
	}

}
