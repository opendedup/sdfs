package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class GetAttributes {

	public Element getResult(String cmd, String file) throws IOException {
		String internalPath = Main.volume.getPath() + File.separator + file;
		File f = new File(internalPath);
		if (!f.exists())
			throw new IOException("requeste file " + file + " does not exist");
		if (f.isDirectory()) {
			try {
				File[] files = f.listFiles();
				Document doc = XMLUtils.getXMLDoc("files");
				Element root = doc.getDocumentElement();
				for (int i = 0; i < files.length; i++) {
					MetaDataDedupFile mf = MetaFileStore.getMF(files[i]
							.getPath());
					Element fe = mf.toXML(doc);
					root.appendChild(fe);
				}
				return (Element) root.cloneNode(true);
			} catch (Exception e) {
				SDFSLogger.getLog().error(
						"unable to fulfill request on file " + file, e);
				throw new IOException(
						"request to fetch attributes failed because "
								+ e.toString());
			}
		} else {
			MetaDataDedupFile mf = MetaFileStore.getMF(internalPath);
			try {
				Document doc = XMLUtils.getXMLDoc("files");
				Element fe = mf.toXML(doc);
				Element root = doc.getDocumentElement();
				root.appendChild(fe);
				return (Element) root.cloneNode(true);
			} catch (Exception e) {
				SDFSLogger.getLog().error(
						"unable to fulfill request on file " + file, e);
				throw new IOException(
						"request to fetch attributes failed because "
								+ e.toString());
			}
		}
	}

}
