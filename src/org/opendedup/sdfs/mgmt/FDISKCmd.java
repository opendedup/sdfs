package org.opendedup.sdfs.mgmt;

import java.io.IOException;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.mtools.BloomFDisk;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.w3c.dom.Element;

public class FDISKCmd implements Runnable {
	int minutes = 0;
	BloomFDisk fd = null;
	String file = null;
	SDFSEvent evt = SDFSEvent.gcInfoEvent("GC Started");

	public Element getResult(String cmd, String file) throws IOException {
		// minutes = Integer.parseInt(cmd);
		this.file = file;
		fd = new BloomFDisk();
		Thread th = new Thread(this);
		th.start();

		try {
			Thread.sleep(1000);
			return evt.toXML();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void run() {
		try {
			fd.init(evt, 0);
		} catch (Exception e) {
			SDFSLogger
					.getLog()
					.error("ERROR Clean Store: unable to cleand dedup storage engine of data not claimed in  ["
							+ minutes + "] because :" + e.toString(), e);

		} finally {
			if(fd !=null)
				fd.vanish();
		}

	}

}
