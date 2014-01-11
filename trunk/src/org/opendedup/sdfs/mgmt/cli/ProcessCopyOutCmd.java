package org.opendedup.sdfs.mgmt.cli;

import java.net.URLEncoder;
import java.util.Formatter;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessCopyOutCmd {
	public static void runCmd(String file, String snapshot) {
		try {
			System.out.printf("taking copy of [%s] destination is [%s]\n",
					file, snapshot);
			file = URLEncoder.encode(file, "UTF-8");
			snapshot = URLEncoder.encode(snapshot, "UTF-8");
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=copyout&options=%s", file, snapshot);
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			formatter.close();
			if (root.getAttribute("status").equals("failed"))
				System.out.println(root.getAttribute("msg"));
			String status = root.getAttribute("status");
			String msg = root.getAttribute("msg");
			System.out.printf("Copy Out [%s] returned [%s]\n", status, msg);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
