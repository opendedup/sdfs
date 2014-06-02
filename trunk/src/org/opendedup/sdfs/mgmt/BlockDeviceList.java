package org.opendedup.sdfs.mgmt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.BlockDev;
import org.w3c.dom.Element;

public class BlockDeviceList {

	public List<Element> getResult() throws Exception {
		if (!Main.blockDev)
			throw new IOException("Block devices not supported on this volume");
		else {
			ArrayList<Element> devs = new ArrayList<Element>();
			for (BlockDev dev : Main.volume.devices) {
				devs.add(dev.getElement());
			}
			return devs;
		}
	}

}
