package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import java.util.List;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.cluster.ClusterSocket;
import org.opendedup.sdfs.cluster.DSEServer;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class GetClusterDSE {

	public Element getResult(String cmd, String file) throws IOException {
		try {
			Document doc = XMLUtils.getXMLDoc("cluster");
			Element root = doc.getDocumentElement();
			ClusterSocket soc = HCServiceProxy.cs;
			List<DSEServer> al = soc.getStorageNodes();
			for (DSEServer s : al) {
				Element el = doc.createElement("dse");
				el.setAttribute("max-size",
						Long.toString(s.maxSize * HCServiceProxy.getPageSize()));
				el.setAttribute(
						"current-size",
						Long.toString(s.currentSize
								* HCServiceProxy.getPageSize()));
				el.setAttribute("free-blocks", Long.toString(s.freeBlocks));
				el.setAttribute("page-size", Long.toString(s.pageSize));
				el.setAttribute("listen-port", Integer.toString(s.dseport));
				el.setAttribute("listen-hostname", s.hostName);
				el.setAttribute("listen-encrypted", Boolean.toString(s.useSSL));
				root.appendChild(el);
			}
			return (Element) root.cloneNode(true);
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to fulfill request on file " + file, e);
			throw new IOException("request to fetch attributes failed because "
					+ e.toString());
		}
	}

}
