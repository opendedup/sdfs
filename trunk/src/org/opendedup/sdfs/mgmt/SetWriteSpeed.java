package org.opendedup.sdfs.mgmt;

import java.io.IOException;




import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.w3c.dom.Element;

public class SetWriteSpeed implements Runnable{
	SDFSEvent evt = null;
	String sz = null;
	public Element getResult(String sz) throws IOException, ParserConfigurationException {
		evt = SDFSEvent.wspEvent("Setting Write Speed");
		this.sz = sz;
		Thread th = new Thread(this);
		th.run();
		return evt.toXML();
		
	}
	@Override
	public void run() {
		try {
			
			int csz = Integer.parseInt(sz);
			HCServiceProxy.setWriteSpeed(csz);
			evt.endEvent("Set Write Speed to " +csz + " KB/s");

		} catch (Exception e) {
			evt.endEvent("unable to fulfill request because " +e.getMessage(), SDFSEvent.ERROR);
			SDFSLogger.getLog().error(
					"unable to fulfill request ",
					e);
		}
		
	}

}
