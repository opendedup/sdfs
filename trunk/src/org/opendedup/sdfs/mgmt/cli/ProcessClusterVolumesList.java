package org.opendedup.sdfs.mgmt.cli;

import java.util.Formatter;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class ProcessClusterVolumesList {
	public static void runCmd() {
		try {
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=cluster-volumes", "null");
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			if (root.getAttribute("status").equals("failed"))
				System.out.println(root.getAttribute("msg"));
			else {
				Element dse = (Element) root.getElementsByTagName(
						"remote-volumes").item(0);
				NodeList nl = dse.getElementsByTagName("volume");
				for (int i = 0; i < nl.getLength(); i++) {
					Element el = (Element) nl.item(i);
					System.out.println(el.getAttribute("name"));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		runCmd();
	}

}
