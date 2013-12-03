package org.opendedup.sdfs.mgmt.cli;

import java.net.URLEncoder;
import java.util.Formatter;

import org.opendedup.logging.SDFSLogger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessDeleteFileCmd {
	String status;
	String msg;

	public static ProcessDeleteFileCmd execute(String file) {
		ProcessDeleteFileCmd store = new ProcessDeleteFileCmd();
		try {
			file = URLEncoder.encode(file, "UTF-8");
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			SDFSLogger.getLog().debug("Deleting File [" + file + "] ");
			formatter.format("file=%s&cmd=%s&options=%s", file, "deletefile",
					"");
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			store.status = root.getAttribute("status");
			store.msg = root.getAttribute("msg");

		} catch (Exception e) {
			store.status = "failed";
			store.msg = e.getMessage();
		}
		return store;
	}

}
