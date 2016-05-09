package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.filestore.cloud.FileReplicationService;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.FileLock;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class GetCloudMetaFile {

	MetaDataDedupFile mf = null;

	private static FileLock fl = new FileLock();
	File df = null;
	SDFSEvent fevt = null;

	public Element getResult(String file, String dstfile) throws IOException {
		ReentrantLock l = fl.getLock(file);
		l.lock();
		try {
			fevt = SDFSEvent.cfEvent(file);
			if (dstfile != null && file.contentEquals(dstfile))
				throw new IOException(
						"local filename in the same as source name");

			File df = null;
			if (dstfile != null)
				df = new File(Main.volume.getPath() + File.separator + dstfile);
			if (df != null && df.exists())
				throw new IOException(dstfile + " already exists");
			try {
				Document doc = XMLUtils.getXMLDoc("cloudmfile");
				Element root = doc.getDocumentElement();

				fevt.maxCt = 4;
				fevt.curCt = 1;
				fevt.shortMsg = "Downloading [" + file + "]";
				mf = FileReplicationService.getMF(file);
				mf.setLocalOwner(false);
				mf.sync();
				fevt.endEvent("retrieved file " + file);
				return (Element) root.cloneNode(true);
			} catch (IOException e) {

				if (mf != null) {
					MetaFileStore.removeMetaFile(mf.getPath(), true);
				}
				fevt.endEvent("unable to get file " + file, SDFSEvent.ERROR);
				throw e;
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to get file " + file, e);
				fevt.endEvent("unable to get file " + file, SDFSEvent.ERROR);
				throw new IOException(
						"request to fetch attributes failed because "
								+ e.toString());

			}
		} finally {
			fl.removeLock(file);
		}
	}

}
