package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import org.opendedup.sdfs.Main;
import org.opendedup.util.SDFSLogger;
import org.opendedup.util.XMLUtils;

public class GetVolume implements XtendedCmd {

	public String getResult(String cmd, String file) throws IOException {
		try {
			return XMLUtils.toXMLString(Main.volume.toXMLDocument());
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to fulfill request on file " + file, e);
			throw new IOException("request to fetch attributes failed because "
					+ e.toString());
		}
	}

}
