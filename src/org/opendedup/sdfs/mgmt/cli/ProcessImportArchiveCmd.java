package org.opendedup.sdfs.mgmt.cli;

import java.io.IOException;

import java.net.URLEncoder;
import java.util.Formatter;

import org.opendedup.util.SDFSLogger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessImportArchiveCmd {
	public static void runCmd(String archive, String path,boolean quiet) throws IOException {
			SDFSLogger.getBasicLog().debug("importing ["+archive+"] destination is ["+path+"]");
			archive = URLEncoder.encode(archive, "UTF-8");
			path = URLEncoder.encode(path, "UTF-8");
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=importarchive&options=%s", archive, path);
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			if (root.getAttribute("status").equals("failed"))
				SDFSLogger.getBasicLog().error("msg");
			String status = root.getAttribute("status");
			String msg = root.getAttribute("msg");
			SDFSLogger.getBasicLog().info("Copy Out ["+status+"] returned ["+msg+"]");
			if(status.equalsIgnoreCase("failed"))
				throw new IOException("Import failed because " + msg);

	}

}
