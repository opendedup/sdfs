package org.opendedup.sdfs.mgmt.cli;

import java.util.Formatter;

import org.opendedup.sdfs.io.BlockDev;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class ProcessBlockDeviceList {
	public static void runCmd() {
		try {
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=s&cmd=blockdev-list");
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			if (root.getAttribute("status").equalsIgnoreCase("success")) {
				NodeList lst = root.getElementsByTagName("blockdev");
				if (lst.getLength() == 0) {
					System.out.println("No Block Devices in Volume");
				} else {
					for (int i = 0; i < lst.getLength(); i++) {
						Element el = (Element) lst.item(i);
						System.out.println(BlockDev.toExternalTxt(el));
						System.out.println();
					}
				}
			} else
				System.out.println(root.getAttribute("msg"));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
