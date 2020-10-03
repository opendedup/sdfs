package org.opendedup.sdfs.mgmt;

import java.io.IOException;
import java.util.List;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.filestore.cloud.CloudRaidStore;
import org.opendedup.sdfs.filestore.cloud.CloudRaidStore.BucketStats;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class GetRaidVolumes {

	public Element getResult() throws IOException {
		try {
			Document doc = XMLUtils.getXMLDoc("buckets");
			Element root = doc.getDocumentElement();
			List<BucketStats> l = CloudRaidStore.getBucketSizes();
			if(l != null) {
				for(BucketStats vl : l) {
					Element el = doc.createElement("bucket");
					el.setAttribute("id", Byte.toString(vl.id));
					el.setAttribute("size", Long.toString(vl.usage.get()));
					el.setAttribute("capacity", Long.toString(vl.capacity.get()));
					el.setAttribute("available", Boolean.toString(vl.connected));
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
