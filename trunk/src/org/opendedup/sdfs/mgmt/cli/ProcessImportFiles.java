package org.opendedup.sdfs.mgmt.cli;

import java.net.URLEncoder;

import java.util.Formatter;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessImportFiles {
	public static void runCmd(int minutes) {
		try {
			String file = URLEncoder.encode("null", "UTF-8");
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			System.out.printf("Cleaning store of data older that [%d] minutes",
					minutes);
			formatter.format("file=%s&cmd=%s&options=%s", file, "cleanstore",
					Integer.toString(minutes));
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			String status = root.getAttribute("status");
			String msg = root.getAttribute("msg");
			System.out.printf("Clean store command [%s] returned [%s]\n",
					status, msg);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
