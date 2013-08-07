package org.opendedup.sdfs.mgmt;

import java.io.IOException;



import org.opendedup.logging.SDFSLogger;
import org.opendedup.mtools.ClusterRedundancyCheck;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.w3c.dom.Element;

public class ClusterRedundancyCmd implements Runnable {
	SDFSEvent evt = null;

	public Element getResult(String cmd, String file) throws IOException {
		this.evt = SDFSEvent.crckInfoEvent(
				"Cluster Redundancy Check Initialized");
		Thread th = new Thread(this);
		th.start();
		try {
			
			Thread.sleep(300);
			return evt.toXML();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void run() {
		try {

			new ClusterRedundancyCheck(this.evt);
		} catch (Exception e) {
			SDFSLogger
					.getLog()
					.error("ERROR Cluster Redundancy Check because :" + e.toString(), e);

		}

	}

}
