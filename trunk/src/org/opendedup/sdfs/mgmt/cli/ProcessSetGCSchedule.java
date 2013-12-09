package org.opendedup.sdfs.mgmt.cli;

import java.net.URLEncoder;

import java.util.Formatter;

import org.w3c.dom.Document;
import org.w3c.dom.Element;


public class ProcessSetGCSchedule {
	public static void runCmd(String schedule) {
		try {
			schedule = URLEncoder.encode(schedule, "UTF-8");
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=set-gc-schedule", schedule);
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			if (root.getAttribute("status").equals("failed"))
				System.out.println(root.getAttribute("msg"));
			else {
				ProcessGetGCSchedule.runCmd();
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	

}
