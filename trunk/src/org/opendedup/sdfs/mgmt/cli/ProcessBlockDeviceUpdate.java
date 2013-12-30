package org.opendedup.sdfs.mgmt.cli;

import java.net.URLEncoder;
import java.util.Formatter;

import org.opendedup.sdfs.io.BlockDev;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessBlockDeviceUpdate {
	public static void runCmd(String devName,String param,String value) {
		try {
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=s&cmd=blockdev-add&devname=%s&param=%s&value=%s", URLEncoder.encode(devName,"UTF-8"),URLEncoder.encode(param,"UTF-8"),value);
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			System.out.println(root.getAttribute("msg"));
			if(root.getAttribute("status").equalsIgnoreCase("success"))
				System.out.println(BlockDev.toExternalTxt((Element)root.getElementsByTagName("blockdev").item(0)));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
