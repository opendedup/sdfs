package org.opendedup.sdfs.mgmt.cli;

import java.util.Formatter;


import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessClusterPromoteToGC {
	public static void runCmd(String vol) {
		try {
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=cluster-promote-gc", vol);
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			System.out.println(root.getAttribute("msg"));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
