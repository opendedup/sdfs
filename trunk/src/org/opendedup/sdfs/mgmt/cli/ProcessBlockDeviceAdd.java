package org.opendedup.sdfs.mgmt.cli;

import java.net.URLEncoder;
import java.util.Formatter;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessBlockDeviceAdd {
	public static void runCmd(String devName,String size,boolean start) {
		try {
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=s&cmd=blockdev-add&devname=%s&size=%s&start=%s", URLEncoder.encode(devName,"UTF-8"),URLEncoder.encode(size,"UTF-8"),Boolean.toString(start));
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			System.out.println(root.getAttribute("msg"));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
