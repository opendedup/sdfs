package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.notification.SDFSEvent;

public class GetEvent {

	public SDFSEvent getResult(String uuid) throws IOException {
		try {
			return SDFSEvent.getEvent(uuid);
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to fulfill request on uuid " + uuid, e);
			throw new IOException("request to fetch attributes failed because "
					+ e.toString());
		}
	}

}
