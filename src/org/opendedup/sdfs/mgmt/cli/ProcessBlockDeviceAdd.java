package org.opendedup.sdfs.mgmt.cli;

import java.net.URLEncoder;
import java.util.Formatter;

import org.opendedup.sdfs.io.BlockDev;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessBlockDeviceAdd {
	public static void runCmd(String devName, String size, boolean start) {
		try {
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format(
					"file=s&cmd=blockdev-add&devname=%s&size=%s&start=%s",
					URLEncoder.encode(devName, "UTF-8"),
					URLEncoder.encode(size, "UTF-8"), Boolean.toString(start));
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			formatter.close();
			System.out.println(root.getAttribute("msg"));
			if (root.getAttribute("status").equalsIgnoreCase("success"))
				System.out.println(BlockDev.toExternalTxt((Element) root
						.getElementsByTagName("blockdev").item(0)));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
