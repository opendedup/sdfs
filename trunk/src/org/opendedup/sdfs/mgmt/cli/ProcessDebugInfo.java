package org.opendedup.sdfs.mgmt.cli;


import java.util.Formatter;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessDebugInfo {
	public static void runCmd() {
		try {

			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=debug-info", "null");
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			Element debug = (Element) root.getElementsByTagName("debug").item(0);
			System.out.printf("Active Threads : %s\n", debug.getAttribute("active-threads"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		runCmd();
	}

}
