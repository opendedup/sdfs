package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class CloseFile {

	public static MetaDataDedupFile lastClosedFile = null;

	public Element getResult(String cmd, String file, long fd) throws IOException {
		try {
			if (fd != -1) {
				DedupFileChannel ch = OpenFile.OpenChannels.remove(fd);
				ch.getDedupFile().unRegisterChannel(ch, -33);
			}
			Document doc = XMLUtils.getXMLDoc("close-file");
			Element root = doc.getDocumentElement();
			File f = new File(Main.volume.getPath() + File.separator + file);

			MetaDataDedupFile mf = MetaFileStore.getMF(f);
			mf.setRetentionLock();
			mf.setDirty(true);
			mf.unmarshal();
			mf.getDedupFile(true).forceClose();
			if (f.exists() && f.getName().endsWith("F1.img")) {
				lastClosedFile = mf;
			}
			return (Element) root.cloneNode(true);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to fulfill request on file " + file, e);
			throw new IOException("request to fetch attributes failed because " + e.toString());
		}
	}

}
