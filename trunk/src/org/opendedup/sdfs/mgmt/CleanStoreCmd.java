package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.filestore.gc.ManualGC;
import org.w3c.dom.Element;

public class CleanStoreCmd implements Runnable {
	int minutes = 0;

	public Element getResult(String cmd, String file) throws IOException {
		minutes = Integer.parseInt(cmd);
		Thread th = new Thread(this);
		th.start();
		try {
			Thread.sleep(300);
			return ManualGC.evt.toXML();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void run() {
		try {

			long chunks = ManualGC.clearChunks(minutes);
			

			SDFSLogger.getLog().info(
					"cleanded dedup storage engine of [" + chunks
							+ "] records not claimed in  [" + minutes + "] ");
		} catch (Exception e) {
			SDFSLogger
					.getLog()
					.error("ERROR Clean Store: unable to cleand dedup storage engine of data not claimed in  ["
							+ minutes + "] because :" + e.toString(), e);

		}

	}

}
