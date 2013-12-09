package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class GetGCSchedule {

	public Element getResult(String cmd, String file) throws IOException {
		try {
			Document doc = XMLUtils.getXMLDoc("schedule");
			Element root = doc.getDocumentElement();
			root.setAttribute("schedule",Main.fDkiskSchedule);
			return (Element) root.cloneNode(true);
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to fulfill request on file " + file, e);
			throw new IOException("request to fetch attributes failed because "
					+ e.toString());
		}
	}
}
