package org.opendedup.sdfs.mgmt.cli;

import java.text.DecimalFormat;
import java.util.Formatter;

import org.opendedup.util.StorageUnit;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.bethecoder.ascii_table.ASCIITable;
import com.bethecoder.ascii_table.ASCIITableHeader;

public class ProcessClusterDSEInfo {
	public static void runCmd() {
		try {
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=cluster-dse-info", "null");

			Document doc = MgmtServerConnection.getResponse(sb.toString());
			formatter.close();
			Element root = doc.getDocumentElement();
			if (root.getAttribute("status").equals("failed"))
				System.out.println(root.getAttribute("msg"));
			else {
				ASCIITableHeader[] headerObjs = {
						new ASCIITableHeader("Host Name", ASCIITable.ALIGN_LEFT),
						new ASCIITableHeader("ID", ASCIITable.ALIGN_LEFT),
						new ASCIITableHeader("Current Size"),
						new ASCIITableHeader("Max Size",
								ASCIITable.ALIGN_CENTER),
						new ASCIITableHeader("Percent Full",
								ASCIITable.ALIGN_LEFT),
						new ASCIITableHeader("Page Size",
								ASCIITable.ALIGN_RIGHT),
						new ASCIITableHeader("Blocks Available for Reuse",
								ASCIITable.ALIGN_RIGHT),
						new ASCIITableHeader("Listen Port",
								ASCIITable.ALIGN_RIGHT),
						new ASCIITableHeader("Rack", ASCIITable.ALIGN_RIGHT),
						new ASCIITableHeader("Location", ASCIITable.ALIGN_RIGHT)

				};

				Element cluster = (Element) root
						.getElementsByTagName("cluster").item(0);
				NodeList dses = cluster.getElementsByTagName("dse");
				String[][] data = new String[dses.getLength()][8];
				for (int i = 0; i < dses.getLength(); i++) {
					Element dse = (Element) dses.item(i);
					long maxSz = Long.parseLong(dse.getAttribute("max-size"));
					long currentSz = Long.parseLong(dse
							.getAttribute("current-size"));
					long freeBlocks = Long.parseLong(dse
							.getAttribute("free-blocks"));
					int pageSize = Integer.parseInt(dse
							.getAttribute("page-size"));
					int port = Integer
							.parseInt(dse.getAttribute("listen-port"));
					String host = dse.getAttribute("listen-hostname");
					double pFull = 0.00;
					if (currentSz > 0) {
						pFull = (((double) currentSz / (double) maxSz) * 100);
						DecimalFormat twoDForm = new DecimalFormat("#.##");
						pFull = Double.valueOf(twoDForm.format(pFull));
					}
					String[] row = { host, dse.getAttribute("id"),
							StorageUnit.of(currentSz).format(currentSz),
							StorageUnit.of(maxSz).format(maxSz),
							Double.toString(pFull), Integer.toString(pageSize),
							Long.toString(freeBlocks), Integer.toString(port),
							dse.getAttribute("rack"),
							dse.getAttribute("location") };
					data[i] = row;
				}
				ASCIITable.getInstance().printTable(headerObjs, data);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		runCmd();
	}

}
