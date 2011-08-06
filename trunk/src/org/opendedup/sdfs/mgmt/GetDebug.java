package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.SDFSLogger;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class GetDebug implements XtendedCmd {

	public String getResult(String cmd, String file) throws IOException {
		try {
			Document doc = XMLUtils.getXMLDoc("debug");
			Element root = doc.getDocumentElement();
			root.setAttribute("active-threads",
					Integer.toString(Thread.activeCount()));
			root.setAttribute(
					"blocks-stored",
					Long.toString(HCServiceProxy.getSize()
							/ HCServiceProxy.getPageSize()));
			root.setAttribute(
					"max-blocks-stored",
					Long.toString(HCServiceProxy.getMaxSize()
							/ HCServiceProxy.getPageSize()));
			return XMLUtils.toXMLString(doc);
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to fulfill request on file " + file, e);
			throw new IOException("request to fetch attributes failed because "
					+ e.toString());
		}
	}

}
