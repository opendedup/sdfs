package org.opendedup.sdfs.mgmt.cli;

import java.net.URLEncoder;
import java.util.Formatter;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessSetPerfmonCmd {
	public static void runCmd(String option) {
		try {
			System.out.printf("Setting perfmon [%s]\n",
					option);
			option = URLEncoder.encode(option, "UTF-8");
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=perfmon&options=%s", "eeks", option);
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			String status = root.getAttribute("status");
			String msg = root.getAttribute("msg");
			System.out
					.printf("Dedup Command [%s] returned [%s]\n", status, msg);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
