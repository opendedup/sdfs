package org.opendedup.sdfs.mgmt.cli;

import java.util.Formatter;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessSyncFSsize {
	public static void runCmd() {
		try {
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=syncfssize", "null");
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			formatter.close();
			if (root.getAttribute("status").equals("failed"))
				System.out.println(root.getAttribute("msg"));
			else {
				if (root.getAttribute("status").equals("failed")) {
					System.out.println(root.getAttribute("msg"));
					System.exit(-1);
				}
				String status = root.getAttribute("status");
				String msg = root.getAttribute("msg");
				System.out
						.printf("Set Cache [%s] returned [%s]\n", status, msg);
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
