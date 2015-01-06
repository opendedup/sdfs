package org.opendedup.sdfs.mgmt;

import java.io.IOException;




import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.w3c.dom.Element;

public class SetCacheSize implements Runnable{
	SDFSEvent evt = null;
	String sz = null;
	public Element getResult(String sz) throws IOException, ParserConfigurationException {
		evt = SDFSEvent.cszEvent("Setting Cache Size");
		this.sz = sz;
		Thread th = new Thread(this);
		th.start();
		return evt.toXML();
		
	}

	@Override
	public void run() {
		try {
			
			long csz = Long.parseLong(sz);
			HCServiceProxy.setCacheSize(csz);
			evt.endEvent("Set Cache Size to " +csz + " bytes");

		} catch (Exception e) {
			evt.endEvent("unable to fulfill request because " +e.getMessage(), SDFSEvent.ERROR);
			SDFSLogger.getLog().error(
					"unable to fulfill request ",
					e);
		}
		
	}

}
