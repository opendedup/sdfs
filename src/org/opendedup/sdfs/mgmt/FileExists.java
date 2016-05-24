package org.opendedup.sdfs.mgmt;

import java.io.File;

import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class FileExists {
	public Element getResult(String cmd, String file) throws IOException {
		try {
			Document doc = XMLUtils.getXMLDoc("file-exists");
			Element root = doc.getDocumentElement();
			File f = new File(Main.volume.getPath() + File.separator + file);
			root.setAttribute("exists", Boolean.toString(f.exists()));
			return (Element) root.cloneNode(true);
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to fulfill request on file " + file, e);
			throw new IOException("request to fetch attributes failed because "
					+ e.toString());
		}
	}
}
