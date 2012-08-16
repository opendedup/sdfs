package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import org.opendedup.sdfs.servers.HashChunkService;
import org.opendedup.util.SDFSLogger;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class GetDSE {

	public Element getResult(String cmd, String file) throws IOException {
		try {
			Document doc = XMLUtils.getXMLDoc("dse");
			Element root = doc.getDocumentElement();
			root.setAttribute(
					"max-size",
					Long.toString(HashChunkService.getMaxSize()
							* HashChunkService.getPageSize()));
			root.setAttribute(
					"current-size",
					Long.toString(HashChunkService.getSize()
							* HashChunkService.getPageSize()));
			root.setAttribute("free-blocks",
					Long.toString(HashChunkService.getFreeBlocks()));
			root.setAttribute("page-size",
					Long.toString(HashChunkService.getPageSize()));
			return (Element)root.cloneNode(true);
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to fulfill request on file " + file, e);
			throw new IOException("request to fetch attributes failed because "
					+ e.toString());
		}
	}

}
