package org.opendedup.sdfs.mgmt;

import java.io.IOException;



import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.OSValidator;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.sun.management.UnixOperatingSystemMXBean;

import java.lang.management.ManagementFactory;


import java.io.File;

public class GetDebug {

	public Element getResult(String cmd, String file) throws IOException {
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
			
			File f = new File(Main.chunkStore);
			root.setAttribute("total-space", Long.toString(f.getTotalSpace()));
			root.setAttribute("free-space", Long.toString(f.getFreeSpace()));
			if(OSValidator.isUnix()) {
				UnixOperatingSystemMXBean perf =  (UnixOperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();
				root.setAttribute("total-cpu-load", Double.toString(perf.getSystemLoadAverage()));
				root.setAttribute("sdfs-cpu-load", Double.toString(perf.getProcessCpuLoad()));
				root.setAttribute("total-memory", Long.toString(Runtime.getRuntime().maxMemory()));
				root.setAttribute("free-memory", Long.toString(Runtime.getRuntime().freeMemory()));
			}
			
			
			
			return (Element)root.cloneNode(true);
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to fulfill request on file " + file, e);
			throw new IOException("request to fetch attributes failed because "
					+ e.toString());
		}
	}

}
