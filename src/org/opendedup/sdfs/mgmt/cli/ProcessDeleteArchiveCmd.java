package org.opendedup.sdfs.mgmt.cli;

import java.net.URLEncoder;

import java.util.Formatter;

import org.opendedup.sdfs.mgmt.cli.MgmtServerConnection;
import org.opendedup.util.SDFSLogger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessDeleteArchiveCmd {
	String status;
	String msg;
	public static ProcessDeleteArchiveCmd execute(String file) {
		ProcessDeleteArchiveCmd store = new ProcessDeleteArchiveCmd();
		try {
			file = URLEncoder.encode(file, "UTF-8");
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			SDFSLogger.getLog().debug("Deleting File ["+file+"] ");
			formatter.format("file=%s&cmd=%s&options=%s", file, "deletearchive",
					"");
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			store.status = root.getAttribute("status");
			store.msg = root.getAttribute("msg");
			
		} catch (Exception e) {
			store.status ="failed";
			store.msg = e.getMessage();
		}
		return store;
	}

}
