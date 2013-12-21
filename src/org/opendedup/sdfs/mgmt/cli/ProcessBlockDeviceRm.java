package org.opendedup.sdfs.mgmt.cli;

import java.net.URLEncoder;
import java.util.Formatter;

import org.opendedup.sdfs.io.BlockDev;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessBlockDeviceRm {
	public static void runCmd(String devName) {
		try {
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=s&cmd=blockdev-rm&devname=%s", URLEncoder.encode(devName,"UTF-8"));
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
