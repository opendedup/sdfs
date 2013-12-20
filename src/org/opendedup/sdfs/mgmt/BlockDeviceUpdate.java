package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.BlockDev;
import org.opendedup.util.StringUtils;
import org.w3c.dom.Element;

public class BlockDeviceUpdate {
	
	public Element getResult(String devName,String size,String start) throws Exception {
		if(!Main.blockDev)
			throw new IOException("Block devices not supported on this volume");
		else {
			BlockDev dev = Main.volume.getBlockDev(devName);
			if(size != null)
				dev.setSize(StringUtils.parseSize(size));
			if(start != null && start.trim().length() > 0)
				dev.setStartOnInit(Boolean.parseBoolean(start));
			Main.volume.writeUpdate();
			return dev.getElement();
		}
	}

}
