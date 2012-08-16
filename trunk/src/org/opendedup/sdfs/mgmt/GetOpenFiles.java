package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.DedupFile;
import org.opendedup.sdfs.io.SparseDedupFile;
import org.opendedup.util.SDFSLogger;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class GetOpenFiles {

	public Element getResult(String cmd, String file) throws IOException {
		try {
			
			Document doc = XMLUtils.getXMLDoc("open-files");
			Element root = doc.getDocumentElement();
			DedupFile[] files = DedupFileStore.getArray();
			root.setAttribute("size", Integer.toString(files.length));
				for (int i = 0; i < files.length; i++) {
					SparseDedupFile df = (SparseDedupFile)files[i];
						Element el = doc.createElement("file");
						el.setAttribute("name", df.getMetaFile().getPath());
						el.setAttribute("last-accessed", Long.toString(MetaFileStore.getMF(df.getMetaFile().getPath())
					.getLastAccessed()));
						el.setAttribute("open-channels", Integer.toString(df.openChannelsSize()));
						root.appendChild(el);
				}
			return (Element)root.cloneNode(true);
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to fulfill request on file " + file, e);
			throw new IOException("request to fetch attributes failed because "
					+ e.toString());
		}
	}

}
