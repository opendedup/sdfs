package org.opendedup.sdfs.mgmt.cli;

import java.net.URLEncoder;
import java.util.Formatter;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessDedupAllCmd {
	public static void runCmd(String file, String option) {
		try {
			System.out.printf("Setting dedup for file [%s] to [%s]\n", file,
					option);
			file = URLEncoder.encode(file, "UTF-8");
			option = URLEncoder.encode(option, "UTF-8");
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=dedup&options=%s", file, option);
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			String status = root.getAttribute("status");
			String msg = root.getAttribute("msg");
			System.out
					.printf("Dedup Command [%s] returned [%s]\n", status, msg);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
