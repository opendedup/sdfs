package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.w3c.dom.Element;

public class SnapshotCmd implements Runnable {

	String srcPath;
	String dstPath;
	SDFSEvent evt;

	public Element getResult(String cmd, String file) throws IOException {
		this.srcPath = file;
		this.dstPath = cmd;
		File f = new File(Main.volume.getPath() + File.separator + srcPath);
		evt = SDFSEvent.snapEvent("Snapshot Intiated for " + this.srcPath
				+ " to " + this.dstPath, f);
		Thread th = new Thread(this);
		th.start();
		try {
			SDFSLogger.getLog().info(evt.toXML());
			return evt.toXML();
		} catch (ParserConfigurationException e) {
			throw new IOException(e);
		}
	}

	private String takeSnapshot(String srcPath, String dstPath)
			throws IOException {
		File f = new File(Main.volume.getPath() + File.separator + srcPath);
		File nf = new File(Main.volume.getPath() + File.separator + dstPath);

		try {
			if (!f.exists())
				throw new IOException("Path not found [" + srcPath + "]");
			if (nf.exists())
				throw new IOException("Path already exists [" + dstPath + "]");
			MetaFileStore.snapshot(f.getPath(), nf.getPath(), false, evt);
			return "Took snapshot of Source [" + srcPath + "] "
					+ " to Destination [" + dstPath + "]";
		} catch (Exception e) {

			throw new IOException(e);
		}
	}

	@Override
	public void run() {
		try {
			takeSnapshot(this.srcPath, this.dstPath);
			evt.endEvent("took snapshot Source [" + srcPath + "] "
					+ "Destination [" + dstPath + "]");
		} catch (IOException e) {
			SDFSLogger.getLog().error(
					"Unable to take snapshot Source [" + srcPath + "] "
							+ "Destination [" + dstPath + "] because :"
							+ e.getMessage(), e);
			evt.endEvent("Unable to take snapshot Source [" + srcPath + "] "
					+ "Destination [" + dstPath + "]", SDFSEvent.ERROR, e);
		}

	}

}
