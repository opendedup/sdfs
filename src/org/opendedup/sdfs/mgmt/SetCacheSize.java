package org.opendedup.sdfs.mgmt;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.w3c.dom.Element;

public class SetCacheSize implements Runnable {
	public SDFSEvent evt = null;
	String sz = null;
	long csz;

	public Element getResult(String sz) throws IOException,
			ParserConfigurationException, Exception {
		evt = SDFSEvent.cszEvent("Setting Cache Size");
		this.sz = sz;
		csz = Long.parseLong(sz);
		Thread th = new Thread(this);
		th.start();
		
		
		return evt.toXML();

	}

	public void setCache(long sz) {
		evt = SDFSEvent.cszEvent("Setting Cache Size to " + sz);
		this.csz = sz;
		Thread th = new Thread(this);
		th.start();
	}

	@Override
	public void run() {
		try {
			HCServiceProxy.setCacheSize(csz);
			Main.volume.writeUpdate();
			evt.endEvent("Set Cache Size to " + csz + " bytes");
		} catch (Exception e) {
			evt.endEvent("unable to fulfill request because " + e.getMessage(),
					SDFSEvent.ERROR);
			SDFSLogger.getLog().error("unable to fulfill request ", e);
		}
	}

}
