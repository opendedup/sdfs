package org.opendedup.sdfs.mgmt.cli;

import java.net.URLEncoder;
import java.util.Formatter;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessFlushBuffersCmd {
	public static void runCmd(String option, String file) {
		try {
			file = URLEncoder.encode(file, "UTF-8");
			option = URLEncoder.encode(option, "UTF-8");
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=flush&options=%s", file, option);
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			formatter.close();
			Element root = doc.getDocumentElement();
			String status = root.getAttribute("status");
			String msg = root.getAttribute("msg");
			System.out
					.printf("Flush Command [%s] returned [%s]\n", status, msg);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
