package org.opendedup.sdfs.mgmt.cli;

import java.util.Formatter;


import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.bethecoder.ascii_table.ASCIITable;
import com.bethecoder.ascii_table.ASCIITableHeader;

public class ProcessConnectedVolumes {
	public static void runCmd() {
		try {

			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=connectedvolumes", "null");
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			//System.out.println(XMLUtils.toXMLString(doc));
			formatter.close();
			if (root.getAttribute("status").equals("failed"))
				System.out.println(root.getAttribute("msg"));
			else {
				ASCIITableHeader[] headerObjs = {
						new ASCIITableHeader("Host Name", ASCIITable.ALIGN_LEFT),
						new ASCIITableHeader("ID", ASCIITable.ALIGN_LEFT),
						new ASCIITableHeader("Size"),
						new ASCIITableHeader("Compressed Size"),
						new ASCIITableHeader("cli port"),
						new ASCIITableHeader("local volume",
								ASCIITable.ALIGN_CENTER)

				};
				
				Element volumes = (Element) root.getElementsByTagName("volumes")
						.item(0);
				NodeList nl = volumes.getElementsByTagName("volume");
				String[][] data = new String[nl.getLength()][headerObjs.length];
				for(int i = 0; i< nl.getLength();i++) {
					Element el = (Element)nl.item(i);
					String[] row = {el.getAttribute("hostname"),el.getAttribute("id"),el.getAttribute("size"),el.getAttribute("compressed-size"),el.getAttribute("port"),el.getAttribute("local")};
					data[i] = row;
				}
				ASCIITable.getInstance().printTable(headerObjs, data);
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		runCmd();
	}

}
