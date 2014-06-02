package org.opendedup.buse.sdfsdev;

import org.opendedup.sdfs.io.BlockDev;

public class BlockDeviceOpenEvent {
	BlockDev dev;

	public BlockDeviceOpenEvent(BlockDev dev) {
		this.dev = dev;
	}

}
