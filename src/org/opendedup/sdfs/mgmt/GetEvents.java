package org.opendedup.sdfs.mgmt;

import java.io.IOException;



import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.SDFSLogger;

public class GetEvents implements XtendedCmd {

	public String getResult(String cmd, String file) throws IOException {
		try {
			return SDFSEvent.getXMLEvents();
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to fulfill request on file " + file, e);
			throw new IOException("request to fetch attributes failed because "
					+ e.toString());
		}
	}

}
