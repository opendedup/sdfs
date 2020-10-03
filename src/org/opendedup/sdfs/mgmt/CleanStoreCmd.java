package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.filestore.gc.ManualGC;
import org.w3c.dom.Element;

public class CleanStoreCmd implements Runnable {
	private boolean compact;
	public Element getResult(boolean compact) throws IOException {
		this.compact = compact;
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

			long chunks = ManualGC.clearChunks(compact);

			SDFSLogger.getLog().info("Expunged [" + chunks + "] unclaimed compact [" + this.compact + "]");
		} catch (Exception e) {
			SDFSLogger
					.getLog()
					.error("ERROR Clean Store: unable to cleand dedup storage engine of data not claimed in because :"
							+ e.toString(), e);

		}

	}

}
