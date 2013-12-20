package org.opendedup.sdfs.mgmt.cli;

import java.net.URLEncoder;
import java.util.Formatter;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessBlockDeviceStop {
	public static void runCmd(String devName,String size) {
		try {
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=s&cmd=blockdev-stop&devname=%s", URLEncoder.encode(devName,"UTF-8"));
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			System.out.println(root.getAttribute("msg"));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
