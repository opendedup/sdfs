package org.opendedup.sdfs.mgmt;

import java.io.IOException;








import org.opendedup.logging.SDFSLogger;
import org.opendedup.mtools.SyncFS;
import org.w3c.dom.Element;

public class SyncFSCmd implements Runnable {
	int minutes = 0;
	SyncFS fd = null;

	public Element getResult() throws IOException {
		//minutes = Integer.parseInt(cmd);
			fd = new SyncFS("now");
		Thread th = new Thread(this);
		th.start();
		try {
			return fd.getEvt().toXML();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void run() {
		try {
			fd.init();
		} catch (Exception e) {
			SDFSLogger
					.getLog()
					.error("ERROR Clean Store: unable to cleand dedup storage engine of data not claimed in  ["
							+ minutes + "] because :" + e.toString(), e);

		}

	}

}
