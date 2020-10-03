package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.w3c.dom.Element;

public class SetReadSpeed implements Runnable {
	public SDFSEvent evt = null;
	int csz = 0;

	public Element getResult(String sz) throws IOException,
			ParserConfigurationException {
		csz = Integer.parseInt(sz);
		evt = SDFSEvent.rspEvent("Setting Read Speed");
		Thread th = new Thread(this);
		th.start();
		return evt.toXML();

	}

	public void setSpeed(int speedInKbs) {
		this.csz = speedInKbs;
		evt = SDFSEvent.rspEvent("Setting Read Speed");
		Thread th = new Thread(this);
		th.start();
	}

	@Override
	public void run() {
		try {
			HCServiceProxy.setReadSpeed(csz);
			evt.endEvent("Set Read Speed to " + csz + " KB/s");

		} catch (Exception e) {
			evt.endEvent("unable to fulfill request because " + e.getMessage(),
					SDFSEvent.ERROR);
			SDFSLogger.getLog().error("unable to fulfill request ", e);
		}

	}

}
