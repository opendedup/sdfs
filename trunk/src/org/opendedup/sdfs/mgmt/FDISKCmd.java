package org.opendedup.sdfs.mgmt;

import java.io.IOException;






import org.opendedup.logging.SDFSLogger;
import org.opendedup.mtools.FDiskException;
import org.opendedup.mtools.FFDisk;
import org.w3c.dom.Element;

public class FDISKCmd implements Runnable {
	int minutes = 0;
	FFDisk fd = null;
	String file = null;

	public Element getResult(String cmd, String file) throws IOException {
		//minutes = Integer.parseInt(cmd);
		this.file = file;
		try {
			fd = new FFDisk(file);
		} catch (FDiskException e1) {
			throw new IOException(e1);
		}
		Thread th = new Thread(this);
		th.start();
		try {
			return fd.fEvt.toXML();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void run() {
		try {
			fd.init(file);
		} catch (Exception e) {
			SDFSLogger
					.getLog()
					.error("ERROR Clean Store: unable to cleand dedup storage engine of data not claimed in  ["
							+ minutes + "] because :" + e.toString(), e);

		}

	}

}
