package org.opendedup.sdfs.mgmt.cli;

import java.io.IOException;

import java.net.URLEncoder;
import java.util.Formatter;

import org.opendedup.logging.SDFSLogger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessImportArchiveCmd {
	public static void runCmd(String archive, String path,String server,String password,int port,boolean quiet,int maxSz) throws IOException {
			SDFSLogger.getBasicLog().debug("importing ["+archive+"] destination is ["+path+"]" + " server is [" + server +"] server password is [" + password + "] server port is [" + port + "]");
			archive = URLEncoder.encode(archive, "UTF-8");
			path = URLEncoder.encode(path, "UTF-8");
			server = URLEncoder.encode(server, "UTF-8");
			password = URLEncoder.encode(password, "UTF-8");
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=importarchive&options=%s&server=%s&spasswd=%s&port=%s&maxsz=%s", archive, path,server,password,Integer.toString(port),Integer.toString(maxSz));
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
