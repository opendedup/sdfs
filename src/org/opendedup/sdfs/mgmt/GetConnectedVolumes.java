package org.opendedup.sdfs.mgmt;

import java.io.IOException;



import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.cloud.FileReplicationService;
import org.opendedup.sdfs.filestore.cloud.RemoteVolumeInfo;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class GetConnectedVolumes {

	public Element getResult() throws IOException {
		try {
			Document doc = XMLUtils.getXMLDoc("volumes");
			Element root = doc.getDocumentElement();
			RemoteVolumeInfo [] l = FileReplicationService.getConnectedVolumes();
			if(l != null) {
				for(RemoteVolumeInfo vl : l) {
					Element el = doc.createElement("volume");
					el.setAttribute("id", Long.toString(vl.id));
					if(vl.id == Main.DSEID)
						el.setAttribute("local", "true");
					else
						el.setAttribute("local", "false");
					el.setAttribute("hostname", vl.hostname);
					el.setAttribute("port", Integer.toString(vl.port));
					el.setAttribute("size", Long.toString(vl.data));
					el.setAttribute("compressed-size", Long.toString(vl.compressed));
					el.setAttribute("sdfsversion", vl.sdfsversion);
					el.setAttribute("version",Integer.toString(vl.version));
					el.setAttribute("lastupdated", Long.toString(vl.lastupdated));
					root.appendChild(el);
				}
			}
			return (Element) root.cloneNode(true);
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to fulfill request", e);
			throw new IOException("request to fetch attributes failed because "
					+ e.toString());
		}
	}

}
