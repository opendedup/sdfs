package org.opendedup.sdfs.mgmt;

import java.io.IOException;



import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.w3c.dom.Element;

public class GetEvents {

	public Element getResult() throws IOException {
		try {
			return SDFSEvent.getXMLEvents();
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to fulfill request ", e);
			throw new IOException("request to fetch attributes failed because "
					+ e.toString());
		}
	}

}
