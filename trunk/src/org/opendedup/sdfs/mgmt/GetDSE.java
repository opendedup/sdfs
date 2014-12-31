package org.opendedup.sdfs.mgmt;

import java.io.IOException;


import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class GetDSE {

	public Element getResult(String cmd, String file) throws IOException {
		try {
			Document doc = XMLUtils.getXMLDoc("dse");
			Element root = doc.getDocumentElement();
			if (HashFunctionPool.max_hash_cluster == 1)
				root.setAttribute(
						"max-size",
						Long.toString(HCServiceProxy.getMaxSize()
								* HCServiceProxy.getPageSize()));
			else
				root.setAttribute(
						"max-size",
						Long.toString(HCServiceProxy.getMaxSize()
								* HashFunctionPool.avg_page_size));
			root.setAttribute("current-size",
					Long.toString(HCServiceProxy.getChunkStore().size()));
			root.setAttribute("compressed-size", Long.toString(HCServiceProxy
					.getChunkStore().compressedSize()));
			root.setAttribute("free-blocks",
					Long.toString(HCServiceProxy.getFreeBlocks()));
			root.setAttribute("page-size",
					Long.toString(HCServiceProxy.getPageSize()));
			root.setAttribute("storage-type", Main.chunkStoreClass);
			root.setAttribute("listen-port", Integer.toString(Main.serverPort));
			root.setAttribute("listen-hostname", Main.serverHostName);
			root.setAttribute("read-speed", Integer.toString(HCServiceProxy.getReadSpeed()));
			root.setAttribute("write-speed", Integer.toString(HCServiceProxy.getWriteSpeed()));
			root.setAttribute("cache-size", Long.toString(HCServiceProxy.getCacheSize()));
			root.setAttribute("max-cache-size", Long.toString(HCServiceProxy.getMaxCacheSize()));
			root.setAttribute("listen-encrypted",
					Boolean.toString(Main.serverUseSSL));
			return (Element) root.cloneNode(true);
		} catch (Exception e) {
			throw new IOException("request to fetch attributes failed because "
					+ e.toString());
		}
	}

}
