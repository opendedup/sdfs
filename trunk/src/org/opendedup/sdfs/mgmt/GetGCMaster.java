package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.cluster.cmds.FindGCMasterCmd;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class GetGCMaster {

	public Element getResult() throws IOException {
		String master = null;
		try {
			if (Main.chunkStoreLocal)
				throw new IOException("Chunk Store is local");
			FindGCMasterCmd acmd = new FindGCMasterCmd();
			acmd.executeCmd(HCServiceProxy.cs);
			master = acmd.getResults().toString();
			Document doc = XMLUtils.getXMLDoc("master");
			Element root = doc.getDocumentElement();
			root.setAttribute("host", master);
			return (Element) root.cloneNode(true);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to fulfill request ", e);
			throw new IOException("unable to fulfill request because "
					+ e.toString());
		}
	}
}
