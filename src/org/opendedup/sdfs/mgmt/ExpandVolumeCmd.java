package org.opendedup.sdfs.mgmt;

import java.io.IOException;


import org.opendedup.sdfs.Main;
import org.opendedup.util.SDFSLogger;

public class ExpandVolumeCmd implements XtendedCmd {

	@Override
	public String getResult(String cmd, String sizeStr) throws IOException {
		try {
			Main.volume.setCapacity(sizeStr);
			return "Volume Expanded to  [" + sizeStr + "]";
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to fulfill request to expand volume to " + sizeStr, e);
			throw new IOException("request to expand volume to [" + sizeStr + "] failed because "
					+ e.toString());
		}
	}
}
