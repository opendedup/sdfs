package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.w3c.dom.Element;

public class GetEvent {

	public Element getResult(String uuid) throws IOException {
		try {
			return SDFSEvent.getXMLEvent(uuid);
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to fulfill request on uuid " + uuid, e);
			throw new IOException("request to fetch attributes failed because "
					+ e.toString());
		}
	}

}
