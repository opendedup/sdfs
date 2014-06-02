package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.BlockDev;
import org.opendedup.util.StringUtils;
import org.w3c.dom.Element;

public class BlockDeviceUpdate {

	public Element getResult(String devName, String param, String value)
			throws Exception {
		if (!Main.blockDev)
			throw new IOException("Block devices not supported on this volume");
		else {
			BlockDev dev = Main.volume.getBlockDev(devName);
			if (param.equalsIgnoreCase("size")) {
				long sz = StringUtils.parseSize(value);
				dev.setSize(sz);
			}
			if (param.equalsIgnoreCase("autostart"))
				dev.setStartOnInit(Boolean.parseBoolean(value));
			Main.volume.writeUpdate();
			return dev.getElement();
		}
	}

}
