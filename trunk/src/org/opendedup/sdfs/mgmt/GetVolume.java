package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.w3c.dom.Element;

public class GetVolume {

	public Element getResult(String cmd, String file) throws IOException {
		try {
			return (Element) Main.volume.toXMLDocument().getDocumentElement()
					.cloneNode(true);
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to fulfill request on file " + file, e);
			throw new IOException("request to fetch attributes failed because "
					+ e.toString());
		}
	}

}
