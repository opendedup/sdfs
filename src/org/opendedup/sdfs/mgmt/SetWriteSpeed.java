package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;

public class SetWriteSpeed implements Runnable {
	public SDFSEvent evt = null;
	int csz;

	public SDFSEvent getResult(String sz) throws IOException,
			ParserConfigurationException {
		evt = SDFSEvent.wspEvent("Setting Write Speed");
		csz = Integer.parseInt(sz);
		Thread th = new Thread(this);
		th.run();
		return evt;

	}

	public void setSpeed(int speedInKbs) {
		csz = speedInKbs;
		evt = SDFSEvent.wspEvent("Setting Write Speed to " + speedInKbs);
		Thread th = new Thread(this);
		th.run();

	}

	@Override
	public void run() {
		try {

			HCServiceProxy.setWriteSpeed(csz);
			evt.endEvent("Set Write Speed to " + csz + " KB/s");

		} catch (Exception e) {
			evt.endEvent("unable to fulfill request because " + e.getMessage(),
					SDFSEvent.ERROR);
			SDFSLogger.getLog().error("unable to fulfill request ", e);
		}

	}

}
