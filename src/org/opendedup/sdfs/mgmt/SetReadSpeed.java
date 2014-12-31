package org.opendedup.sdfs.mgmt;

import java.io.IOException;




import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.w3c.dom.Element;

public class SetReadSpeed implements Runnable{
	SDFSEvent evt = null;
	String sz = null;
	public Element getResult(String sz) throws IOException, ParserConfigurationException {
		this.sz = sz;
		evt = SDFSEvent.rspEvent("Setting Read Speed");
		Thread th = new Thread(this);
		th.start();
		return evt.toXML();
		
	}

	@Override
	public void run() {
		try {
			
			int csz = Integer.parseInt(sz);
			HCServiceProxy.setReadSpeed(csz);
			evt.endEvent("Set Read Speed to " +csz + " KB/s");
			

		} catch (Exception e) {
			evt.endEvent("unable to fulfill request because " +e.getMessage(), SDFSEvent.ERROR);
			SDFSLogger.getLog().error(
					"unable to fulfill request ",
					e);
		}
		
	}

}
